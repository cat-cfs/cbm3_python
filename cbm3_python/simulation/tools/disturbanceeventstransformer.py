# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

class DisturbanceEventsTransformer(object):
    
    def makeDisturbanceEvents(self, project, sourceTable):
        #self.__createDisturbanceEventsTable(project)
        
        DisturbanceEventID = project.GetMaxID("tblDisturbanceEvents", "DisturbanceEventID") + 1

        for row in project.Query("""SELECT * FROM {0}""".format(sourceTable)).fetchall():
            sql = """
                  INSERT INTO tblDisturbanceEvents (
                      DisturbanceEventID,
                      DisturbanceGroupScenarioID,
                      SVOID,
                      TimeStepStart,
                      TimeStepFinish,
                      Efficiency,
                      SortingCondition,
                      DistFormat,
                      DistArea,
                      MerchCarbonToDisturb,
                      PropOfRecordToDisturb,
                      ClassifierSetID,
                      CriteriaID,
                      OptionID,
                      OverrideStaticTR)
                  VALUES (
                    {DisturbanceEventID},
                    {DisturbanceGroupScenarioID},
                    {SVOID},
                    {TimeStepStart},
                    {TimeStepFinish},
                    {Efficiency},
                    {SortingCondition},
                    {DistFormat},
                    {DistArea},
                    {MerchCarbonToDisturb},
                    {PropOfRecordToDisturb},
                    {ClassifierSetID},
                    {CriteriaID},
                    {OptionID},
                    {OverrideStaticTR})""".format(
                        DisturbanceEventID = DisturbanceEventID,
                        DisturbanceGroupScenarioID = row.de_DisturbanceGroupScenarioID,
                        SVOID = row.SVOID,
                        TimeStepStart = row.TimeStepStart,
                        TimeStepFinish = row.TimeStepFinish,
                        Efficiency = row.Efficiency,
                        SortingCondition = row.SortingCondition,
                        DistFormat = row.DistFormat,
                        DistArea = row.DistArea,
                        MerchCarbonToDisturb = row.MerchCarbonToDisturb,
                        PropOfRecordToDisturb = row.PropOfRecordToDisturb,
                        ClassifierSetID = row.de_ClassifierSetID,
                        CriteriaID = row.de_CriteriaID,
                        OptionID = row.OptionID,
                        OverrideStaticTR = row.OverrideStaticTR,
                        )

            project.ExecuteQuery(sql)
            DisturbanceEventID = DisturbanceEventID + 1
  
    def __createDisturbanceEventsTable(self, project):
        queries = [
            "SELECT * INTO _tblDisturbanceEvents FROM tblDisturbanceEvents",
            "DROP TABLE tblDisturbanceEvents",
            """
            CREATE TABLE tblDisturbanceEvents (
                DisturbanceEventID AUTOINCREMENT PRIMARY KEY,
                DisturbanceGroupScenarioID INTEGER,
                SVOID INTEGER,
                TimeStepStart INTEGER,
                TimeStepFinish INTEGER,
                Efficiency DOUBLE,
                SortingCondition INTEGER,
                DistFormat INTEGER,
                DistArea DOUBLE,
                MerchCarbonToDisturb DOUBLE,
                PropOfRecordToDisturb DOUBLE,
                ClassifierSetID INTEGER,
                CriteriaID INTEGER,
                OptionID INTEGER,
                OverrideStaticTR YESNO)
            """,
            """
            INSERT INTO tblDisturbanceEvents (
                DisturbanceEventID,
                DisturbanceGroupScenarioID,
                SVOID,
                TimeStepStart,
                TimeStepFinish,
                Efficiency,
                SortingCondition,
                DistFormat,
                DistArea,
                MerchCarbonToDisturb,
                PropOfRecordToDisturb,
                ClassifierSetID,
                CriteriaID,
                OptionID,
                OverrideStaticTR)
            SELECT
                DisturbanceEventID,
                DisturbanceGroupScenarioID,
                SVOID,
                TimeStepStart,
                TimeStepFinish,
                Efficiency,
                SortingCondition,
                DistFormat,
                DistArea,
                MerchCarbonToDisturb,
                PropOfRecordToDisturb,
                ClassifierSetID,
                CriteriaID,
                OptionID,
                OverrideStaticTR
            FROM _tblDisturbanceEvents
            """,
            "DROP TABLE _tblDisturbanceEvents"]
        
        for sql in queries:
            project.ExecuteQuery(sql)
