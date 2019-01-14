# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

from cbm3data.accessdb import AccessDB
import logging

def prepare_afforestation_db(db_path, start_year, end_year):
    with AccessDB(db_path) as a:
        logging.info("running afforestation data preparations fixes")
        sql_run_length = """
            UPDATE tblRunTableDetails
            SET RunLength = {endYear} - ({startYear} - 1)
            WHERE RunID = (select max(runid) from tblSimulation);
            """.format(endYear=end_year, startYear=start_year)

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

def run_af_results_fixes(af_rrdb_path):
    #MH, 20 sept 2017: Gary's CBM fix for the afforestation project uses a non-UFCCC land class code (23), but the rest
    #                  of the NIR process uses the correct UNFCCC landclass 1 (CLCL) so we need to change 23 back to 1
    #                  for the post-proicessing and rollup to work as intended.

    #doing these update queries in batches since pyodbc has trouble running them all at once
    sql_groups=[
        {
            "table": "tblAgeIndicators",
            "id_col": "AgeIndID",
            "queries": [
                "UPDATE tblAgeIndicators SET LandClassID=1 where LandClassID=23 and AgeIndID Between ? And ?;",
                "UPDATE tblAgeIndicators SET TimeStep = TimeStep-20, kf3 = kf3-20, kf4 = kf4-20 where AgeIndID Between ? And ?;",
                "UPDATE tblAgeIndicators SET kf4 = 0 WHERE kf4 < 1990 and AgeIndID Between ? And ?;"
            ]
        },
        {
            "table": "tblDistIndicators",
            "id_col": "DistIndID",
            "queries": [
                "UPDATE tblDistIndicators SET LandClassID=1 where LandClassID=23 and DistIndID Between ? And ?;",
                "UPDATE tblDistIndicators SET TimeStep = TimeStep-20, kf3 = kf3-20, kf4 = kf4-20 where DistIndID Between ? And ?;",
                "UPDATE tblDistIndicators SET kf4 = 0 WHERE kf4 < 1990 and DistIndID Between ? And ?;",

            ]
        },
        {
            "table": "tblFluxIndicators",
            "id_col": "FluxIndicatorID",
            "queries": [
                "UPDATE tblFluxIndicators SET LandClassID=1 where LandClassID=23 and FluxIndicatorID Between ? And ?;",
                "UPDATE tblFluxIndicators SET TimeStep = TimeStep-20, kf3 = kf3-20, kf4 = kf4-20 where FluxIndicatorID Between ? And ?;",
                "UPDATE tblFluxIndicators SET kf4 = 0 WHERE kf4 < 1990 and FluxIndicatorID Between ? And ?;"
            ]
        },
        {
            "table": "tblPoolIndicators",
            "id_col": "PoolIndID",
            "queries": [
                "UPDATE tblPoolIndicators SET LandClassID=1 where LandClassID=23 and PoolIndID Between ? And ?;",
                "UPDATE tblPoolIndicators SET TimeStep = TimeStep-20, kf3 = kf3-20, kf4 = kf4-20 where PoolIndID Between ? And ?;",
                "UPDATE tblPoolIndicators SET kf4 = 0 WHERE kf4 < 1990 and PoolIndID Between ? And ?;",
            ]
        }
        
        #"""UPDATE tblNIRSpecialOutput
        #   SET LandClass_From=1 where LandClass_From=23;""",
        #"""UPDATE tblNIRSpecialOutput 
        #   SET TimeStep = TimeStep-20, CalendarYear = CalendarYear-20;"""
    ]
    logging.info("running afforestation results fixes on {}".format(af_rrdb_path))
    with AccessDB(af_rrdb_path, False) as rrdb:
        for sql_group in sql_groups:
            logging.info(sql_group["table"])
            max_id = rrdb.GetMaxID(sql_group["table"], sql_group["id_col"])
            maxBatchDeleteSize = 50000
            iterations = max_id / maxBatchDeleteSize
            remainder = max_id % maxBatchDeleteSize
            ranges = [{
                "min": x*maxBatchDeleteSize,
                "max": x*maxBatchDeleteSize+maxBatchDeleteSize-1 #between is inclusive
                } for x in range(iterations)]
            if remainder>0:
               ranges.append({
                   "min": iterations * maxBatchDeleteSize,
                   "max": iterations * maxBatchDeleteSize + remainder
                })
            for r in ranges:
                for sql in sql_group["queries"]:
                    rrdb.ExecuteQuery(query=sql, params=(r["min"], r["max"]))

    logging.info("finished running afforestation results fixes.")

