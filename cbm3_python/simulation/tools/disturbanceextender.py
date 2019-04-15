# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

# core
import os
import logging

class DisturbanceExtender(object):
    '''
    Author: Max Fellows

    - 2014 Jun 13, modified by Scott for NET_NET_2014 python scripting

    Extends a selection of disturbance events up to a specified year using a
    specified year as a template. Does not do anything extra, just copies the
    template rows and ensures that the timestepstart and timestepfinish fields
    are correct and that disturbanceeventid is unique.
    '''

    def Run(self, project, extensions):
        for event in extensions:
            if not self.__checkForEvent(event, project):
                logging.info(
                    """
                    Tried to extend {0} from {1},
                    but no occurrences found in that year.
                    """.format(event.getTitle(), event.getFromYear()))
                continue

            logging.info("Extending {0} from {1} to {2}".format(
                event.getTitle(),
                event.getFromYear(),
                event.getToYear()))

            maxEventId = project.GetMaxID("tbldisturbanceevents", "disturbanceeventid")

            extraTimesteps = event.getToYear() - event.getFromYear()
            for timestep in range(1, extraTimesteps + 1):
                for table in ("disturbance_event_additions", "dist_event_increments"):
                    if project.tableExists(table):
                        project.ExecuteQuery("DROP TABLE {0}".format(table))

                timestepStart = event.getFromYear() - 1989
                sql = \
                    """
                    SELECT tblDisturbanceEvents.*
                    INTO disturbance_event_additions
                    FROM (
                        tblDisturbanceType
                        INNER JOIN tblDisturbanceGroupScenario
                            ON tblDisturbanceType.DistTypeID = tblDisturbanceGroupScenario.DistTypeID
                    ) INNER JOIN tblDisturbanceEvents
                        ON tblDisturbanceGroupScenario.DisturbanceGroupScenarioID
                            = tblDisturbanceEvents.DisturbanceGroupScenarioID
                    IN '{0}'
                    WHERE tblDisturbanceType.DefaultDistTypeID IN ({1})
                        AND tblDisturbanceEvents.TimeStepStart = {2}
                    """.format(project.path,
                                ",".join([str(a) for a in event.getDefaultDistTypeIDs()]),
                                timestepStart)

                project.ExecuteQuery(sql)

                project.ExecuteQuery(
                    """
                    CREATE TABLE dist_event_increments (
                        row_num AUTOINCREMENT CONSTRAINT  dist_event_increments_pk PRIMARY KEY,
                        disturbanceeventid integer)
                    """)


                incrementTableSql = \
                    """
                    INSERT into dist_event_increments (disturbanceeventid)
                    SELECT DISTINCT(events.disturbanceeventid)
                    FROM disturbance_event_additions events
                    """

                project.ExecuteQuery(incrementTableSql)

                updateTimestepSql = \
                    """
                    UPDATE disturbance_event_additions events
                    SET events.timestepstart = events.timestepstart + {0},
                        events.timestepfinish = events.timestepfinish + {0}
                    """.format(timestep)

                project.ExecuteQuery(updateTimestepSql)

                updateDistTypeSql = \
                    """
                    UPDATE disturbance_event_additions events
                    INNER JOIN dist_event_increments increment
                        ON events.disturbanceeventid = increment.disturbanceeventid
                    SET events.disturbanceeventid = {0} + increment.row_num
                    """.format(maxEventId)

                project.ExecuteQuery(updateDistTypeSql)

                appendSql = \
                    """
                    INSERT INTO tblDisturbanceEvents IN '{0}'
                    SELECT * FROM disturbance_event_additions
                    """.format(project.path)

                countNewRows = project.Query(
                    """
                    SELECT Count(DisturbanceEventID) AS newEvents
                    FROM disturbance_event_additions;
                    """).fetchone().newEvents

                project.ExecuteQuery(appendSql)
                for table in ("disturbance_event_additions", "dist_event_increments"):
                    if project.tableExists(table):
                        project.ExecuteQuery("DROP TABLE {0}".format(table))
                maxEventId += countNewRows


    def __checkForEvent(self, event, db):
        '''
        Checks that a disturbance event occurs in the target database for the
        year it should be extended from.

        :param event: the event to check for
        :type event: :class:`.DisturbanceExtension`
        :param db: connection to the target database
        :type db: :class:`.AccessDatabase`
        '''
        sql = \
            """
            SELECT TOP 1 *
            FROM (
                tblDisturbanceType
                INNER JOIN tblDisturbanceGroupScenario
                    ON tblDisturbanceType.DistTypeID = tblDisturbanceGroupScenario.DistTypeID
            ) INNER JOIN tblDisturbanceEvents
                ON tblDisturbanceGroupScenario.DisturbanceGroupScenarioID
                    = tblDisturbanceEvents.DisturbanceGroupScenarioID
            WHERE tblDisturbanceType.DefaultDistTypeID IN ({0})
                AND tblDisturbanceEvents.TimeStepStart = {1}
            """.format(",".join([str(a) for a in event.getDefaultDistTypeIDs()]),
                       event.getFromYear() - 1989)

        result = db.Query(sql)

        if result is not None:
            return result.fetchone() is not None
        else:
            return False