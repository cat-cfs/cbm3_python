
def sql_delete_disturbance_events(default_dist_type_ids):
    """
    Gets a CBM3 project db query for deleting all 
    disturbance events that have one of the specified
    default disturbance type Ids.
    """
    return ("""
    Delete tblDisturbanceEvents.*
    FROM tblDisturbanceType INNER JOIN (tblDisturbanceGroupScenario 
    INNER JOIN tblDisturbanceEvents 
        ON tblDisturbanceGroupScenario.DisturbanceGroupScenarioID = 
            tblDisturbanceEvents.DisturbanceGroupScenarioID) 
                ON tblDisturbanceType.DistTypeID =
                    tblDisturbanceGroupScenario.DistTypeID
    WHERE tblDisturbanceType.DefaultDistTypeID in({})
    """.format(",".join(['?' for x in default_dist_type_ids])),
    default_dist_type_ids)

def sql_delete_post_year_events(year):
    """
    Gets a CBM3 project db query for deleting all 
    disturbance events after the specified year. 
    This uses the NIR timestep 1 = 1990 assumption.
    """
    return ("""
    delete from tblDisturbanceEvents
    where DisturbanceEventID in
    (
      select tDE.DisturbanceEventID 
        from tblDisturbanceType as tDT inner join 
          (tblDisturbanceEvents as tDE inner join 
            (tblDisturbanceGroupScenario as tDGS inner join
              (
                select distinct tRDSL.DisturbanceGroupScenarioID, tRDSL.RunDisturbanceScenarioID, tRDSL.DistTypeID 
                from tblRunDisturbanceScenarioLookup as tRDSL inner join tblRunTable as tRT on tRT.RunDisturbanceScenarioID = tRDSL.RunDisturbanceScenarioID
                where tRT.RunID = (select max(runID) from tblSimulation)     
              ) as join1
              on tDGS.DisturbanceGroupScenarioID = join1.DisturbanceGroupScenarioID
            )
          on tDE.DisturbanceGroupScenarioID = tDGS.DisturbanceGroupScenarioID
        )
        on tDT.DistTypeID = tDGS.DistTypeID
        where tDE.TimeStepStart+1989 > ?
    )""", year)

def sql_set_run_project_run_length(numTimeStep):
    return ("""
        UPDATE tblRunTableDetails SET tblRunTableDetails.RunLength = ?
        WHERE tblRunTableDetails.RunID In (SELECT DISTINCT Max(tblRunTable.RunID) AS MaxOfRunID
        FROM tblRunTable);
        """, numTimeStep)

def run_simulation_id_cleanup(projectDB):

    queries = [
        "UPDATE tblRunTable SET runID=1;",
        "UPDATE tblRunTableDetails SET runID=1;",
        "DELETE * FROM tblSimulation WHERE runID <> (SELECT max(runID) FROM tblSimulation);",
        "UPDATE tblSimulation SET SimulationID=1;",
        "DELETE * from tblRunDisturbanceScenario as tRDS WHERE tRDS.RunDisturbanceScenarioID <> (SELECT max(RunDisturbanceScenarioID) FROM tblRunTable);",
        "UPDATE tblRunDisturbanceScenario SET RunDisturbanceScenarioID=1;",
        "DELETE * from tblDisturbanceGroupScenario as tDGS WHERE tDGS.DisturbanceGroupScenarioID NOT IN (SELECT distinct DisturbanceGroupScenarioID FROM tblRunDisturbanceScenarioLookup);"
    ]

    for x in queries:
        projectDB.ExecuteQuery(x)

def update_random_seed(projectDB):
    result = projectDB.Query("""
    SELECT tblRandomSeed.CBMRunID, tblRandomSeed.RandomSeed, tblRandomSeed.OnOffSwitch
    FROM tblRandomSeed
    WHERE tblRandomSeed.CBMRunID In (
        SELECT DISTINCT Max(tblRandomSeed.CBMRunID) AS MaxOfRunID
        FROM tblRandomSeed;
        );""").fetchone()
    if result is None or result[0] is None: #throw an error if no random seed was found
        raise Exception("no random seed found")

    #clean out the old random seeds
    projectDB.ExecuteQuery("DELETE FROM tblRandomSeed")

    projectDB.ExecuteQuery("INSERT INTO tblRandomSeed (CBMRunID, RandomSeed, OnOffSwitch) VALUES ({0},{1},{2})"
                    .format(1,result.RandomSeed,True))

def set_classifiers_off(project_db):
    projectDB.ExecuteQuery("UPDATE tblClassifiersOnOff SET IsOn = NO")