# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

from cbm3_python.cbm3data.accessdb import AccessDB
import logging

def run_af_results_fixes(af_rrdb_path):

    #doing these update queries in batches since pyodbc has trouble running them all at once
    sql_groups=[
        {
            "table": "tblAgeIndicators",
            "id_col": "AgeIndID",
            "queries": [
                "UPDATE tblAgeIndicators SET TimeStep = TimeStep-20, kf3 = kf3-20, kf4 = kf4-20 where AgeIndID Between ? And ?;",
                "UPDATE tblAgeIndicators SET kf4 = 0 WHERE kf4 < 1990 and AgeIndID Between ? And ?;"
            ]
        },
        {
            "table": "tblDistIndicators",
            "id_col": "DistIndID",
            "queries": [
                "UPDATE tblDistIndicators SET TimeStep = TimeStep-20, kf3 = kf3-20, kf4 = kf4-20 where DistIndID Between ? And ?;",
                "UPDATE tblDistIndicators SET kf4 = 0 WHERE kf4 < 1990 and DistIndID Between ? And ?;",
            ]
        },
        {
            "table": "tblFluxIndicators",
            "id_col": "FluxIndicatorID",
            "queries": [
                "UPDATE tblFluxIndicators SET TimeStep = TimeStep-20, kf3 = kf3-20, kf4 = kf4-20 where FluxIndicatorID Between ? And ?;",
                "UPDATE tblFluxIndicators SET kf4 = 0 WHERE kf4 < 1990 and FluxIndicatorID Between ? And ?;"
            ]
        },
        {
            "table": "tblPoolIndicators",
            "id_col": "PoolIndID",
            "queries": [
                "UPDATE tblPoolIndicators SET TimeStep = TimeStep-20, kf3 = kf3-20, kf4 = kf4-20 where PoolIndID Between ? And ?;",
                "UPDATE tblPoolIndicators SET kf4 = 0 WHERE kf4 < 1990 and PoolIndID Between ? And ?;",
            ]
        }

    ]
    logging.info("running afforestation results fixes on {}".format(af_rrdb_path))
    with AccessDB(af_rrdb_path, False) as rrdb:

        for sql_group in sql_groups:
            
            logging.info(sql_group["table"])
            ranges = rrdb.get_batched_query_ranges(sql_group["table"],sql_group["id_col"], 50000)
            for r in ranges:
                for sql in sql_group["queries"]:
                    rrdb.ExecuteQuery(query=sql, params=r)

    logging.info("finished running afforestation results fixes.")

