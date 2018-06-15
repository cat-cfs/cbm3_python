from cbm3data.accessdb import AccessDB
import logging

def prepare_afforestation_db(db_path, start_year, end_year):
    with AccessDB(db_path) as a:
        logging.info("running afforestation data preparations fixes")
        sql_run_length = """
            UPDATE tblRunTableDetails
            SET RunLength = {endYear} - ({startYear} - 1)
            WHERE RunID = (select max(runid) from tblSimulation);
            """.format(end_year, start_year)

        # 19-sept-2017: Scott Morken told me that the land classes need to be changed from 1 to 23 before the
        # corrections Gary made to CBM will be triggered. This makes that change.

        # These changes were applied to the AF project as part of scenario 09_NDExclusion_oldrules_newexes, so they don't need to be done again.
        sql_land_class_inventory = """
            UPDATE tblInventory
                    SET LandClass = 23
                    WHERE Landclass = 1;"""

        sql_landclass_stand_init = """UPDATE tblStandInitNonForestLookup
                    SET UNFCCForestType = 23
                    WHERE UNFCCForestType = 1;"""

        sql_landclass_svl_attributes = """
            UPDATE tblSVLAttributes
            SET UNFCCForestType = 23
            WHERE UNFCCForestType = 1;"""

        a.ExecuteQuery(sql_run_length)
        a.ExecuteQuery(sql_land_class_inventory)
        a.ExecuteQuery(sql_landclass_stand_init)
        a.ExecuteQuery(sql_landclass_svl_attributes)
        logging.info("finished running afforestation data preparations fixes")

def run_afforestation_results_fixes(af_rrdb_path):
    #MH, 20 sept 2017: Gary's CBM fix for the afforestation project uses a non-UFCCC land class code (23), but the rest
    #                  of the NIR process uses the correct UNFCCC landclass 1 (CLCL) so we need to change 23 back to 1
    #                  for the post-proicessing and rollup to work as intended.
    sql=[
        "UPDATE tblAgeIndicators SET LandClassID=1 where LandClassID=23;",
        "UPDATE tblAgeIndicators SET TimeStep = TimeStep-20, kf3 = kf3-20, kf4 = kf4-20;",
        "UPDATE tblAgeIndicators SET kf4 = 0 WHERE kf4 < 1990;",

        "UPDATE tblDistIndicators SET LandClassID=1 where LandClassID=23;",
        "UPDATE tblDistIndicators SET TimeStep = TimeStep-20, kf3 = kf3-20, kf4 = kf4-20;",
        "UPDATE tblDistIndicators SET kf4 = 0 WHERE kf4 < 1990;",
        "UPDATE tblFluxIndicators SET LandClassID=1 where LandClassID=23;",

        "UPDATE tblFluxIndicators SET TimeStep = TimeStep-20, kf3 = kf3-20, kf4 = kf4-20;",

        "UPDATE tblFluxIndicators SET kf4 = 0 WHERE kf4 < 1990;",

        "UPDATE tblPoolIndicators SET LandClassID=1 where LandClassID=23;",

        "UPDATE tblPoolIndicators SET TimeStep = TimeStep-20, kf3 = kf3-20, kf4 = kf4-20;",

        "UPDATE tblPoolIndicators SET kf4 = 0 WHERE kf4 < 1990;",

        #"""UPDATE tblNIRSpecialOutput
        #   SET LandClass_From=1 where LandClass_From=23;""",
        #"""UPDATE tblNIRSpecialOutput 
        #   SET TimeStep = TimeStep-20, CalendarYear = CalendarYear-20;"""
    ]

    with AccessDB(af_rrdb_path) as rrdb:
        for q in sql:
            rrdb.ExecuteQuery(q)

