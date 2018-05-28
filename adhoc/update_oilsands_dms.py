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
    os.path.join(local_wd, "DRAFT_DMS_FOR_FIRST_PILOT_STUDY_RUN_DEC_16_2016_VScott.xlsx"),
    2)

dms_by_language = {d["Language"]: d for d in dms}

original_aidbs = [
    { "Language": "English", "Path": r"M:\CBM Tools and Development\Builds\OpScaleArchiveIndex\ArchiveIndex_Beta_Install.mdb" },
    { "Language": "French", "Path": r"M:\CBM Tools and Development\Builds\OpScaleArchiveIndex\ArchiveIndex_Beta_Install_fr.mdb" },
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
        #disturbance types (tblDisturbanceTypeDefault)

        #disturbance matrices (tblDM)

        #disturbance matrix values (tblDMValuesLookup)

        #disturbance matrix association (tblDMAssociationDefault)