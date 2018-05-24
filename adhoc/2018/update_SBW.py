import shutil, os, logging, xlrd
from cbm3data.accessdb import AccessDB

def start_logging(fn=".\\script.log",fmode='w', use_console=True):
    #set up logging to print to console window and to log file
    #
    # From http://docs.python.org/2/howto/logging-cookbook.html#logging-cookbook
    #
    rootLogger = logging.getLogger()

    logFormatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%m-%d %H:%M')

    fileHandler = logging.FileHandler(fn, fmode)
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)
    if use_console:
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logFormatter)
        rootLogger.addHandler(consoleHandler)

    rootLogger.setLevel(logging.INFO)

def load_dict_from_worksheet(sheet):
    # read header values into the list    
    keys = [sheet.cell(0, col_index).value for col_index in xrange(sheet.ncols)]

    dict_list = []
    for row_index in xrange(1, sheet.nrows):
        d = {keys[col_index]: sheet.cell(row_index, col_index).value 
             for col_index in xrange(sheet.ncols)}
        dict_list.append(d)
    return dict_list

start_logging("update_sbw.log")

# load the translations for the disturbance types
wb = xlrd.open_workbook(r"c:\dev\AIDB_SBW_Update\NewDMtranslations.xlsx")
distName_ws = wb.sheet_by_index(0)
distTypeTranslations = load_dict_from_worksheet(distName_ws)
distDesc_ws = wb.sheet_by_index(1)
distDescTranslations = load_dict_from_worksheet(distDesc_ws)

#1 copy the current op scale archive index databases
local_wd = r"C:\dev\AIDB_SBW_Update"

new_sbw_aidb = os.path.join(local_wd, "ArchiveIndex_NIR2018_NDExclusion_newrules_newexes.mdb")

original_aidbs = [
    { "Language": "English", "Path": r"M:\CBM Tools and Development\Builds\OpScaleArchiveIndex\ArchiveIndex_Beta_Install.mdb" },
    { "Language": "Spanish", "Path": r"M:\CBM Tools and Development\Builds\OpScaleArchiveIndex\ArchiveIndex_Beta_Install_es.mdb" },
    { "Language": "French", "Path": r"M:\CBM Tools and Development\Builds\OpScaleArchiveIndex\ArchiveIndex_Beta_Install_fr.mdb" },
    { "Language": "Russian", "Path": r"M:\CBM Tools and Development\Builds\OpScaleArchiveIndex\ArchiveIndex_Beta_Install_ru.mdb" }
]

local_aidb_paths = [os.path.join(local_wd, os.path.basename(x["Path"])) for x in original_aidbs]
logging.info("copying original aidbs from network")
for i in range(len(original_aidbs)):
    logging.info("{0}->{1}".format(original_aidbs[i]["Path"], local_aidb_paths[i]))
    shutil.copy(original_aidbs[i]["Path"], local_aidb_paths[i])

#2 drop the existing SBW matrices and disturbance types
for p in local_aidb_paths:
    logging.info("removing old SBW from {0}".format(p))
    a = AccessDB(p)

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

# 3 add the new SBW disturbance types
source_aidb = AccessDB(new_sbw_aidb)
new_dist_ids = [str(x) for x in list(range(240,267))]
for p in local_aidb_paths:
    logging.info("adding QC SBW to {0}".format(p))
    a = AccessDB(p, False)

    # 3a disturbance types
    logging.info("copying disturbance types to {}".format(p))
    for dist_type_row in source_aidb.Query("SELECT * FROM tblDisturbanceTypeDefault WHERE DistTypeID in({})".format(",".join(new_dist_ids))):
        logging.info(dist_type_row.DistTypeName)
        a.ExecuteQuery("INSERT INTO tblDisturbanceTypeDefault (DistTypeID, DistTypeName, OnOffSwitch, Description, IsStandReplacing, IsMultiYear, MultiYearCount) VALUES (?,?,?,?,?,?,?)", 
                      (dist_type_row.DistTypeID,
                      dist_type_row.DistTypeName,
                      dist_type_row.OnOffSwitch,
                      dist_type_row.Description,
                      dist_type_row.IsStandReplacing,
                      dist_type_row.IsMultiYear,
                      dist_type_row.MultiYearCount))

    # 3b disturbance matrices (tblDM)
    logging.info("copying dmids to {}".format(p))
    for dmrow in source_aidb.Query("""SELECT tblDM.DMID, tblDM.Name, tblDM.Description, tblDM.DMStructureID
            FROM tblDM INNER JOIN tblDMAssociationDefault ON tblDM.DMID = tblDMAssociationDefault.DMID
            WHERE tblDMAssociationDefault.DefaultDisturbanceTypeID in({})
            GROUP BY tblDM.DMID, tblDM.Name, tblDM.Description, tblDM.DMStructureID;
            """.format(",".join(new_dist_ids))):
        logging.info(dmrow.Name)
        a.ExecuteQuery("INSERT INTO tblDM (DMID, Name, Description, DMStructureID) VALUES (?,?,?,?)",
                       (dmrow.DMID,
                       dmrow.Name,
                       dmrow.Description,
                       dmrow.DMStructureID))

    # 3c dm values (tblDMValuesLookup)
    logging.info("copying dmvalues to {}".format(p))
    for dmvaluerow in source_aidb.Query("""SELECT tblDMValuesLookup.DMID, tblDMValuesLookup.DMRow, tblDMValuesLookup.DMColumn, tblDMValuesLookup.Proportion
            FROM tblDMAssociationDefault INNER JOIN tblDMValuesLookup ON tblDMAssociationDefault.DMID = tblDMValuesLookup.DMID
            WHERE tblDMAssociationDefault.DefaultDisturbanceTypeID in ({})
            GROUP BY tblDMValuesLookup.DMID, tblDMValuesLookup.DMRow, tblDMValuesLookup.DMColumn, tblDMValuesLookup.Proportion;
            """.format(",".join(new_dist_ids))):
        logging.info(dmvaluerow)
        a.ExecuteQuery("INSERT INTO tblDMValuesLookup (DMID, DMRow, DMColumn, Proportion) VALUES (?,?,?,?)",
                (dmvaluerow.DMID,
                dmvaluerow.DMRow,
                dmvaluerow.DMColumn,
                dmvaluerow.Proportion))

    # 3d disturbance matrix associations
    logging.info("copying dm associations to {}".format(p))
    for dmassociationRow in source_aidb.Query(""" SELECT tblDMAssociationDefault.DefaultDisturbanceTypeID, tblDMAssociationDefault.DefaultEcoBoundaryID, tblDMAssociationDefault.AnnualOrder, tblDMAssociationDefault.DMID, tblDMAssociationDefault.Name, tblDMAssociationDefault.Description
            FROM tblDMAssociationDefault WHERE tblDMAssociationDefault.DefaultDisturbanceTypeID In({});
            """.format(",".join(new_dist_ids))):
        logging.info(dmassociationRow.Name)
        a.ExecuteQuery("INSERT INTO tblDMAssociationDefault (DefaultDisturbanceTypeID, DefaultEcoBoundaryID, AnnualOrder, DMID, Name, Description) VALUES (?,?,?,?,?,?)",
                       (dmassociationRow.DefaultDisturbanceTypeID,
                        dmassociationRow.DefaultEcoBoundaryID,
                        dmassociationRow.AnnualOrder,
                        dmassociationRow.DMID,
                        dmassociationRow.Name,
                        dmassociationRow.Description))
