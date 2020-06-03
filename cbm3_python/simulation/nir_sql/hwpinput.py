# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import os
import logging


def get_hwp_clearcut_filepath(dir):
    return os.path.join(dir, "hwp_clearcut.in")


def get_hwp_deforestation_filepath(dir):
    return os.path.join(dir, "hwp_deforestation.in")


def get_hwp_firewood_filepath(dir):
    return os.path.join(dir, "hwp_firewood.in")


def __PrintQuery(outputPath, rollupDB, thesql):
    logging.info("Extracting 1990-2030 CBM harvest: \n" + thesql)

    rows = rollupDB.Query(thesql).fetchall()

    logging.info("Writing harvest inputs to " + outputPath)

    with open(outputPath, 'w+') as inf:
        for row in rows:
            inf.write(row[0]+'\n')


def GeneratHWPInput(rollupDB, workingDir):
    for sql, outFile in (
        (r"""
    SELECT '<INITIAL_CMO>Domestic Roundwood Harvest & Salvage in RU'
               & SPUID & ';MW;Green Roundwood;' & str(harvested_tc) & ';'
               & str(tDI.Timestep) & ';;;0;'
               & str(1989+tDI.Timestep) & ' Harvest: Dist type '
               & tDTD.DistTypeID & ' - ' & tDTD.DistTypeName & ' ;'
    FROM (
        SELECT tDI.Timestep, tDI.SPUID, tDTD.DistTypeID, tDTD.DistTypeName,
               sum(DistProduct) as harvested_tc
        from tblDistIndicators as tDI inner join
            tblDisturbanceTypeDefault as tDTD on tDI.DistTypeID =
                tDTD.DistTypeID
        WHERE tDI.SPUID > 0 and tDI.DistTypeID in (4,185,195,196,197,198) and
            tDI.LandClassID = 0 and tDI.DistProduct > 0
        GROUP BY tDI.SPUID, tDI.Timestep, tDTD.DistTypeID, tDTD.DistTypeName
        ORDER BY tDI.Timestep, tDI.SPUID, tDTD.DistTypeName)
    """, get_hwp_clearcut_filepath(workingDir)),
        (r"""
    SELECT '<INITIAL_CMO>Domestic Deforestation in RU'
           & SPUID & ';MW;Green Roundwood from Deforestation;'
           & str(harvested_tc) & ';' & str(tDI.Timestep) & ';;;0;'
           & str(1989+tDI.Timestep) & ' Deforestation Harvest;'
    FROM
    (
        SELECT tDI.Timestep, tDI.SPUID, sum(DistProduct) as harvested_tc
        from tblDistIndicators as tDI inner join
            tblDisturbanceTypeDefault as tDTD on tDI.DistTypeID =
                tDTD.DistTypeID
        WHERE tDI.SPUID > 0 and tDI.DistTypeID Not In (
            3,4,185,195,196,197,198, 236, 237, 238, 239
            ) and tDI.DistProduct > 0
        GROUP BY tDI.SPUID, tDI.Timestep
        ORDER BY tDI.Timestep, tDI.SPUID
    );""", get_hwp_deforestation_filepath(workingDir)),
        (r"""
    SELECT '<INITIAL_CMO>Domestic Firewood in RU' & SPUID & ';MW;Firewood;'
       & str(harvested_tc) & ';' & str(tDI.Timestep) & ';;;0;'
       & str(1989+tDI.Timestep) & ' Firewood Harvest;'
    FROM
    (   SELECT tDI.Timestep, tDI.SPUID, sum(DistProduct) as harvested_tc
        from tblDistIndicators as tDI inner join tblDisturbanceTypeDefault
            as tDTD on tDI.DistTypeID = tDTD.DistTypeID
        WHERE tDI.SPUID > 0 and tDI.DistTypeID in (236, 237, 238, 239)
              and tDI.LandClassID = 0 and tDI.DistProduct > 0
        GROUP BY tDI.SPUID, tDI.Timestep
        ORDER BY tDI.Timestep, tDI.SPUID)
    """, get_hwp_firewood_filepath(workingDir))):
        __PrintQuery(outFile, rollupDB, sql)
