
def sql_delete_disturbance_events(default_dist_type_ids):
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

