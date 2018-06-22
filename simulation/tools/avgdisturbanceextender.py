import logging
from disturbanceeventstransformer import DisturbanceEventsTransformer

class AvgDisturbanceExtender(object):
    '''
    Extends a disturbance event into the future:
        * calculates the average yearly target for the event by SPUGroupID
        * using the criteria and other properties from the most recent
          occurrence of the event in the DisturbanceEvents database, extends
          the disturbance event into the future up to a specified year
        * uses a specified efficiency and sorting condition for the extended
          events
    '''
    
    OUTPUT_TABLE = "new_disturbance_events"

    def __init__(self):
        self.__disturbanceEventsHelper = DisturbanceEventsTransformer()
    
    def Run(self, project, extensions):        
        isFirstEvent = True
        for event in extensions:
            if not self.__checkForEvent(project, event):
                logging.info(
                    """
                    Tried to extend {0} from {1},
                    but no occurrences found in that year.
                    """.format(event.getTitle(), event.getFromYear()))
                continue
                    
            logging.info("Extending {0} from {1} to {2}".format( 
                event.getTitle(), event.getFromYear(), event.getToYear()))
                
            self.__extendEvent(project, event, isFirstEvent)
            isFirstEvent = False
            
            self.__disturbanceEventsHelper.makeDisturbanceEvents(
                project, self.OUTPUT_TABLE)

    def __checkForEvent(self, project, event):
        '''
        Checks that a disturbance event occurs in the target database for the
        year it should be extended from.
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

        result = project.Query(sql).fetchone()
        
        return result is not None
            
    def __extendEvent(self, project, event, isFirstEvent):
        '''
        Extends a disturbance event into the future.
        '''
        idsToExtend = event.getDefaultDistTypeIDs()
        
        # Calculates the average yearly target for the disturbances by SPUGroupID.
        avgTargetsBySpuGroup = self.__getAvgTargetsBySpuGroup(project, idsToExtend)
        for i, groupTarget in enumerate(avgTargetsBySpuGroup):
            isFirstGroup = (i == 0)
            
            # Determines the year to start from when extending disturbances.
            lastYear = groupTarget.lastDisturbedYear
            
            startYear = event.getFromYear() + 1 if event.getFromYear() is not None \
                else lastYear + 1
                
            endYear = event.getToYear()
            
            logging.info(
                "\tSPUGroupID {id} ({start} - {end}), template: {last}".format(
                    id=groupTarget.SPUGroupID,
                    start=startYear,
                    end=endYear,
                    last=lastYear))
            
            for j, year in enumerate(range(startYear, endYear + 1)):
                isFirstYear = (j == 0)
                createTable = (    isFirstEvent
                               and isFirstYear
                               and isFirstGroup)
            
                self.createDisturbanceYear(project=project,
                                           newYear=year - 1989,
                                           templateYear=lastYear - 1989,
                                           defaultDistTypeIds=idsToExtend,
                                           target=groupTarget,
                                           createTable=createTable)

    def __getAvgTargetsBySpuGroup(self, project, defaultDistTypeIds):
        '''
        Calculates the average annual target for the disturbance type from
        the data available.
        '''
        idSqlPlaceholders = ",".join("?" * len(defaultDistTypeIds))
        
        # Calculates the number of years to average the disturbance target over.
        sql = """
              SELECT MIN(1989 + de.TimeStepStart) AS firstDisturbedYear,
                     MAX(1989 + de.TimeStepStart) AS finalDisturbedYear
              FROM tblDisturbanceEvents de
              INNER JOIN (
                tblDisturbanceGroupScenario dgs
                INNER JOIN tblDisturbanceType dt
                    ON dgs.DistTypeID = dt.DistTypeID
              ) ON de.DisturbanceGroupScenarioID = dgs.DisturbanceGroupScenarioID
              WHERE dt.DefaultDistTypeID IN ({})
              """.format(idSqlPlaceholders)
        
        disturbancePeriod = project.Query(sql, defaultDistTypeIds).fetchone()
        firstDisturbedYear = disturbancePeriod.firstDisturbedYear
        finalDisturbedYear = disturbancePeriod.finalDisturbedYear
        yearsToAverage = finalDisturbedYear - firstDisturbedYear + 1
        
        # Calculates the average annual target for the disturbance by SPUGroupID
        # using the calculated number of years.
        sql = """
              SELECT dgs.SPUGroupID,
                     MAX(1989 + de.TimeStepStart) AS lastDisturbedYear,
                     SUM(de.DistArea) / ? AS avgArea,
                     SUM(de.MerchCarbonToDisturb) / ? AS avgMerch
              FROM tblDisturbanceEvents de
              INNER JOIN (
                tblDisturbanceGroupScenario dgs
                INNER JOIN tblDisturbanceType dt
                    ON dgs.DistTypeID = dt.DistTypeID
              ) ON de.DisturbanceGroupScenarioID = dgs.DisturbanceGroupScenarioID
              WHERE dt.DefaultDistTypeID IN ({})
              GROUP BY dgs.SPUGroupID
              """.format(idSqlPlaceholders)
        
        params = []
        params.append(yearsToAverage)
        params.append(yearsToAverage)
        params.extend(defaultDistTypeIds)
        
        avgTargetsBySpuGroup = project.Query(sql, params)
        
        return avgTargetsBySpuGroup.fetchall()

    def createDisturbanceYear(self, project, newYear, templateYear,
                              defaultDistTypeIds, target, createTable):
        '''
        Creates a single new year of disturbances based on the most recent year
        found in the source database prior to being run through this disturbance
        extender.
        '''
        idSqlPlaceholders = ",".join("?" * len(defaultDistTypeIds))
        
        # Copies the most recent rows for the disturbance event to use as a
        # base for rows into the future.
        if createTable:
            sql = """
                  SELECT TOP 1 *
                  INTO {outputTable}
                  FROM tblDisturbanceEvents de
                  INNER JOIN (
                    tblDisturbanceGroupScenario dgs
                    INNER JOIN tblDisturbanceType dt
                        ON dgs.DistTypeID = dt.DistTypeID
                  ) ON de.DisturbanceGroupScenarioID = dgs.DisturbanceGroupScenarioID
                  WHERE dt.DefaultDistTypeID IN ({placeholders})
                      AND de.TimeStepStart + 1989 = ?
                      AND dgs.SPUGroupID = ?
                  """.format(outputTable=self.OUTPUT_TABLE,
                             placeholders=idSqlPlaceholders)
        else:
            sql = """
                  INSERT INTO {outputTable}
                  SELECT TOP 1 *
                  FROM tblDisturbanceEvents de
                  INNER JOIN (
                    tblDisturbanceGroupScenario dgs
                    INNER JOIN tblDisturbanceType dt
                        ON dgs.DistTypeID = dt.DistTypeID
                  ) ON de.DisturbanceGroupScenarioID = dgs.DisturbanceGroupScenarioID
                  WHERE dt.DefaultDistTypeID IN ({placeholders})
                      AND de.TimeStepStart = ?
                      AND dgs.SPUGroupID = ?
                  """.format(outputTable=self.OUTPUT_TABLE,
                             placeholders=idSqlPlaceholders)
        
        params = []
        params.extend(defaultDistTypeIds)
        params.append(templateYear)
        params.append(target.SPUGroupID)
        project.ExecuteQuery(sql, params)
        
        # Updates the newly-copied rows with the next year's data.
        sql = """
              UPDATE {outputTable}
              SET TimeStepStart = ?,
                  TimeStepFinish = ?,
                  DistArea = ?,
                  MerchCarbonToDisturb = ?
              WHERE TimeStepStart = ?
                  AND SPUGroupID = ?
                  AND DefaultDistTypeID IN ({placeholders})
              """.format(outputTable=self.OUTPUT_TABLE,
                         placeholders=idSqlPlaceholders)
        
        params = [newYear, newYear, target.avgArea, target.avgMerch,
                  templateYear, target.SPUGroupID]
                  
        params.extend(defaultDistTypeIds)
        project.ExecuteQuery(sql, params)
