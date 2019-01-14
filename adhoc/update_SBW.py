# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import sys
sys.path.append('../')

#2018 this script updates the Operational scale archive index databases (in En,Es,Fr,Ru) 
#with the NIR Firewood collection and Spruce Budworm (QC) matrices and disturbance types
#and also drops the old SBW matrices, dist types, and association
import shutil, os, logging
from cbm3data.accessdb import AccessDB
from cbm3data.compact_and_repair import *
from util import loghelper
from util import excelhelper

local_wd = r"M:\CBM Tools and Development\CBM3\2018\AIDB_SBW_Update"

loghelper.start_logging(os.path.join(local_wd,"update_sbw.log"))

# load the translations for the disturbance types
distTypeTranslations = excelhelper.get_worksheet_as_dict(os.path.join(local_wd, "NewDMtranslations.xlsx"), 0)
distDescTranslations = excelhelper.get_worksheet_as_dict(os.path.join(local_wd, "NewDMtranslations.xlsx"), 1)
distTypeTranslations = {int(x["DistID"]): x for x in distTypeTranslations}
distDescTranslations = {int(x["DistID"]): x for x in distDescTranslations}

#1 copy the current op scale archive index databases
new_sbw_aidb = os.path.join(local_wd, "ArchiveIndex_NIR2018_NDExclusion_newrules_newexes.mdb")

original_aidbs = [
    { "Language": "English", "Path": r"M:\CBM Tools and Development\Builds\OpScaleArchiveIndex\20170824\ArchiveIndex_Beta_Install.mdb" },
    { "Language": "Spanish", "Path": r"M:\CBM Tools and Development\Builds\OpScaleArchiveIndex\20170824\ArchiveIndex_Beta_Install_es.mdb" },
    { "Language": "French", "Path": r"M:\CBM Tools and Development\Builds\OpScaleArchiveIndex\20170824\ArchiveIndex_Beta_Install_fr.mdb" },
    { "Language": "Russian", "Path": r"M:\CBM Tools and Development\Builds\OpScaleArchiveIndex\20170824\ArchiveIndex_Beta_Install_ru.mdb" }
]
makeLocalPath = lambda path : os.path.join(local_wd, os.path.basename(path))
local_aidbs = [{"Language": x["Language"], "Path": makeLocalPath(x["Path"])} for x in original_aidbs]

logging.info("copying original aidbs from network")
for i in range(len(original_aidbs)):
    origPath = original_aidbs[i]["Path"]
    localPath = local_aidbs[i]["Path"]
    logging.info("{0}->{1}".format(origPath, localPath))
    shutil.copy(origPath, localPath)

#2 drop the existing SBW matrices and disturbance types
for p in local_aidbs:
    logging.info("removing old SBW from {0}".format(p))
    with AccessDB(p["Path"]) as a:

        # 2a get the set of default disturbance types ids, and the set of DMIDs that are associated with, and only with SBW (so that we dont remove 
        res = a.Query("SELECT tblDMAssociationDefault.DefaultDisturbanceTypeID, tblDMAssociationDefault.DMID, tblDMAssociationDefault.Name FROM tblDMAssociationDefault")
        sbw_dist_ids = set()
        sbw_dm_ids = set()
        non_sbw_dist_ids = set()
        non_sbw_dm_ids = set()
        sbw_rows = []
        non_sbw_rows = []
        for r in res:
            if(r.Name.startswith("SBW")):
                sbw_dist_ids.add(r.DefaultDisturbanceTypeID)
                sbw_dm_ids.add(r.DMID)
            else:
                non_sbw_dist_ids.add(r.DefaultDisturbanceTypeID)
                non_sbw_dm_ids.add(r.DMID)

        # do set differences to make sure we dont delete non-sbw DMs or disturbances
        sbw_dist_ids = sbw_dist_ids - non_sbw_dist_ids
        sbw_dm_ids = sbw_dm_ids - non_sbw_dm_ids

        # 2b drop all SBW related rows
        a.ExecuteQuery("delete from tblDMAssociationDefault where Left(NAME,3)='SBW'")
        a.ExecuteQuery("delete from tblDisturbanceTypeDefault where DistTypeId in({0})".format(",".join([str(x) for x in sbw_dist_ids])))
        a.ExecuteQuery("delete from tblDM where DMID in({0})".format(",".join([str(x) for x in sbw_dm_ids])))
        a.ExecuteQuery("delete from tblDMValuesLookup where DMID in({0})".format(",".join([str(x) for x in sbw_dm_ids])))

# 3 load the source data
with AccessDB(new_sbw_aidb) as source_aidb:

    new_dist_ids = [str(x) for x in list(range(236,240))]
    source_tblDisturbanceTypeDefault = list(
        source_aidb.Query("SELECT * FROM tblDisturbanceTypeDefault WHERE DistTypeID in({})".format(",".join(new_dist_ids))))

    source_tblDM = list(source_aidb.Query("""SELECT tblDM.DMID, tblDM.Name, tblDM.Description, tblDM.DMStructureID
                    FROM tblDM INNER JOIN tblDMAssociationDefault ON tblDM.DMID = tblDMAssociationDefault.DMID
                    WHERE tblDMAssociationDefault.DefaultDisturbanceTypeID in({})
                    GROUP BY tblDM.DMID, tblDM.Name, tblDM.Description, tblDM.DMStructureID;
                    """.format(",".join(new_dist_ids))))

    source_tblDMValuesLookup = list(source_aidb.Query("""SELECT tblDMValuesLookup.DMID, tblDMValuesLookup.DMRow, tblDMValuesLookup.DMColumn, tblDMValuesLookup.Proportion
                    FROM tblDMAssociationDefault INNER JOIN tblDMValuesLookup ON tblDMAssociationDefault.DMID = tblDMValuesLookup.DMID
                    WHERE tblDMAssociationDefault.DefaultDisturbanceTypeID in ({})
                    GROUP BY tblDMValuesLookup.DMID, tblDMValuesLookup.DMRow, tblDMValuesLookup.DMColumn, tblDMValuesLookup.Proportion;
                    """.format(",".join(new_dist_ids))))

    source_tblDMAssociationDefault = list(source_aidb.Query(""" SELECT tblDMAssociationDefault.DefaultDisturbanceTypeID, tblDMAssociationDefault.DefaultEcoBoundaryID, tblDMAssociationDefault.AnnualOrder, tblDMAssociationDefault.DMID, tblDMAssociationDefault.Name, tblDMAssociationDefault.Description
                    FROM tblDMAssociationDefault WHERE tblDMAssociationDefault.DefaultDisturbanceTypeID In({})
                    ORDER BY tblDMAssociationDefault.DefaultDisturbanceTypeID, tblDMAssociationDefault.DMID, tblDMAssociationDefault.DefaultEcoBoundaryID"""
                    .format(",".join(new_dist_ids))))

    #pad out the ecoboundaries, allowing users to point at the QC matrices from other regions
    unique_dm_associations = {}
    unique_dm_associations_first_row = {}
    for row in source_tblDMAssociationDefault:
        key = (int(row.DefaultDisturbanceTypeID), (int(row.DMID)))
        if key in unique_dm_associations:
            unique_dm_associations[key].add(int(row.DefaultEcoBoundaryID))
        else:
            unique_dm_associations[key] = set([(int(row.DefaultEcoBoundaryID))])
            unique_dm_associations_first_row[key] = row

    source_ecoBoundary_set = set(int(x[0]) for x in source_aidb.Query("""SELECT tblEcoBoundaryDefault.EcoBoundaryID FROM tblEcoBoundaryDefault;"""))
    for k,v in unique_dm_associations.items():
        missing_ecos = source_ecoBoundary_set - v
        for e in missing_ecos:
            original_row = unique_dm_associations_first_row[k]
            #creating a row through pyodbc with a query since we have all the values already and it's not easy to copy one
            append_row = source_aidb.Query(""" 
                SELECT {DefaultDisturbanceTypeID} as DefaultDisturbanceTypeID,
                {DefaultEcoBoundaryID} as DefaultEcoBoundaryID,
                {AnnualOrder} as AnnualOrder, 
                {DMID} as DMID,
                '{Name}' as Name,
                '{Description}' as Description
                FROM tblDMAssociationDefault""".format(
                    DefaultDisturbanceTypeID=original_row.DefaultDisturbanceTypeID,
                    DefaultEcoBoundaryID=e,
                    AnnualOrder = original_row.AnnualOrder,
                    DMID = original_row.DMID,
                    Name = original_row.Name,
                    Description = original_row.Description,
                    )).fetchone()
            source_tblDMAssociationDefault.append(append_row)



# 4 add the new SBW and firewood disturbance types
for p in local_aidbs:
    logging.info("adding QC SBW to {0}".format(p))
    with AccessDB(p["Path"], False) as a:
        new_dmid_dist_type_combinations = {}

        # 3a disturbance types
        logging.info("copying disturbance types to {}".format(p))
        for dist_type_row in source_tblDisturbanceTypeDefault:
            logging.info(dist_type_row.DistTypeName)
            a.ExecuteQuery("INSERT INTO tblDisturbanceTypeDefault (DistTypeID, DistTypeName, OnOffSwitch, Description, IsStandReplacing, IsMultiYear, MultiYearCount) VALUES (?,?,?,?,?,?,?)", 
                          (dist_type_row.DistTypeID,
                          distTypeTranslations[dist_type_row.DistTypeID][p["Language"]],
                          dist_type_row.OnOffSwitch,
                          distDescTranslations[dist_type_row.DistTypeID][p["Language"]],
                          dist_type_row.IsStandReplacing,
                          dist_type_row.IsMultiYear,
                          dist_type_row.MultiYearCount))

        # 3b disturbance matrices (tblDM)
        logging.info("copying dmids to {}".format(p))
        for dmrow in source_tblDM:
            logging.info(dmrow.Name)
            a.ExecuteQuery("INSERT INTO tblDM (DMID, Name, Description, DMStructureID) VALUES (?,?,?,?)",
                           (dmrow.DMID,
                           dmrow.Name,
                           dmrow.Description,
                           dmrow.DMStructureID))

        # 3c dm values (tblDMValuesLookup)
        logging.info("copying dmvalues to {}".format(p))
        for dmvaluerow in source_tblDMValuesLookup:
            logging.info(dmvaluerow)
            a.ExecuteQuery("INSERT INTO tblDMValuesLookup (DMID, DMRow, DMColumn, Proportion) VALUES (?,?,?,?)",
                    (dmvaluerow.DMID,
                    dmvaluerow.DMRow,
                    dmvaluerow.DMColumn,
                    dmvaluerow.Proportion))

        # 3d disturbance matrix associations
        logging.info("copying dm associations to {}".format(p))
        for dmassociationRow in source_tblDMAssociationDefault:
            logging.info(dmassociationRow.Name)
            new_dmid_dist_type_combination_key = (dmassociationRow.DefaultDisturbanceTypeID, dmassociationRow.DMID)
            if new_dmid_dist_type_combination_key in new_dmid_dist_type_combinations:
                new_dmid_dist_type_combinations[new_dmid_dist_type_combination_key].add(dmassociationRow.DefaultEcoBoundaryID)
            else:
                new_dmid_dist_type_combinations[new_dmid_dist_type_combination_key] = set([dmassociationRow.DefaultEcoBoundaryID])

            a.ExecuteQuery("INSERT INTO tblDMAssociationDefault (DefaultDisturbanceTypeID, DefaultEcoBoundaryID, AnnualOrder, DMID, Name, Description) VALUES (?,?,?,?,?,?)",
                           (dmassociationRow.DefaultDisturbanceTypeID,
                            dmassociationRow.DefaultEcoBoundaryID,
                            dmassociationRow.AnnualOrder,
                            dmassociationRow.DMID,
                            dmassociationRow.Name,
                            dmassociationRow.Description))



for p in local_aidbs:
    logging.info("run compact and repair on {}".format(p["Path"]))
    compact_and_repair(p["Path"])