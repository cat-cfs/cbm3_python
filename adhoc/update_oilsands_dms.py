#this script inserts the Disturbacne types and matrices found at this dir
#M:\CBM Tools and Development\CBM3\2018\Oilsands_DM_Update
#into an archive index database

import shutil, os, logging
from cbm3data.accessdb import AccessDB
from cbm3data.compact_and_repair import *
from util import loghelper
from util import excelhelper

local_wd = r"M:\CBM Tools and Development\CBM3\2018\Oilsands_DM_Update"

loghelper.start_logging(os.path.join(local_wd,"update_oilsands_matrices.log"))

dms = excelhelper.get_worksheet_as_dict(
    os.path.join(local_wd, "Oilsands_DM_update.xlsx"),
    0)

dms_by_language = {}
for d in dms:
    #group by language
    if d["Language"] in dms_by_language:
        dms_by_language[d["Language"]].append(d)
    else:
        dms_by_language[d["Language"]] = [d]

dm_values = excelhelper.get_worksheet_as_dict(
    os.path.join(local_wd, "Oilsands_DM_update.xlsx"),
    1)

dm_values_by_id = {}
for d in dm_values:
    if d["DMID"] in dm_values_by_id:
        dm_values_by_id[d["DMID"]].append(d)
    else:
        dm_values_by_id[d["DMID"]] = [d]

original_aidbs = [
    { "Language": "English", "Path": r"M:\CBM Tools and Development\Builds\OpScaleArchiveIndex\20180525\ArchiveIndex_Beta_Install.mdb" },
    { "Language": "Spanish", "Path": r"M:\CBM Tools and Development\Builds\OpScaleArchiveIndex\20180525\ArchiveIndex_Beta_Install_es.mdb" },
    { "Language": "French", "Path": r"M:\CBM Tools and Development\Builds\OpScaleArchiveIndex\20180525\ArchiveIndex_Beta_Install_fr.mdb" },
    { "Language": "Russian", "Path": r"M:\CBM Tools and Development\Builds\OpScaleArchiveIndex\20180525\ArchiveIndex_Beta_Install_ru.mdb" }
]
makeLocalPath = lambda path : os.path.join(local_wd, os.path.basename(path))
local_aidbs = [{"Language": x["Language"], "Path": makeLocalPath(x["Path"])} for x in original_aidbs]

logging.info("copying original aidbs from network")

for i in range(len(original_aidbs)):
    origPath = original_aidbs[i]["Path"]
    localPath = local_aidbs[i]["Path"]
    logging.info("{0}->{1}".format(origPath, localPath))
    shutil.copy(origPath, localPath)

#add the disturbance types
for p in local_aidbs:
    logging.info("adding oilsands DM to {0}".format(p))
    with AccessDB(p["Path"], False) as a:
        language = p["Language"]

        # get set of eco boundaries. Though the matrices apply to QC only, we are associating it with all ecozones for the time being.
        source_ecozones = [int(x[0]) for x in a.Query("SELECT tblEcoBoundaryDefault.EcoBoundaryID from tblEcoBoundaryDefault")]

        nextDistTypeID = a.GetMaxID("tblDisturbanceTypeDefault", "DistTypeID") + 1
        nextDMID = a.GetMaxID("tblDM", "DMID") + 1
        #disturbance types (tblDisturbanceTypeDefault)
        for r in dms_by_language[language]:
            a.ExecuteQuery("INSERT INTO tblDisturbanceTypeDefault (DistTypeID, DistTypeName, OnOffSwitch, Description, IsStandReplacing, IsMultiYear, MultiYearCount) VALUES (?,?,?,?,?,?,?)", 
                          (nextDistTypeID,
                          r["Name"],
                          True,
                          r["Description"],
                          True,
                          False,
                          0))

            #disturbance matrices (tblDM)
            a.ExecuteQuery("INSERT INTO tblDM (DMID, Name, Description, DMStructureID) VALUES (?,?,?,?)",
                (nextDMID,
                r["Name"],
                r["Description"],
                2))

            dmid = int(r["DMID"])
            dm_value_rows = dm_values_by_id[dmid]
            for dm_value_row in dm_value_rows:
                a.ExecuteQuery("INSERT INTO tblDMValuesLookup (DMID, DMRow, DMColumn, Proportion) VALUES (?,?,?,?)",
                    (nextDMID,
                    int(dm_value_row["DMRow"]),
                    int(dm_value_row["DMColumn"]),
                    dm_value_row["Proportion"]))

            for eco in source_ecozones:
                a.ExecuteQuery("INSERT INTO tblDMAssociationDefault (DefaultDisturbanceTypeID, DefaultEcoBoundaryID, AnnualOrder, DMID, Name, Description) VALUES (?,?,?,?,?,?)",
                            (nextDistTypeID,
                            eco,
                            1,
                            nextDMID,
                            r["Name"],
                            r["Description"]))

            nextDMID += 1
            nextDistTypeID += 1

for p in local_aidbs:
    logging.info("run compact and repair on {}".format(p["Path"]))
    compact_and_repair(p["Path"])