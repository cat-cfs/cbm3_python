import os, shutil
from cbm3data.accessdb import AccessDB
from cbm3data.aidb import AIDB
from simulation.simulator import Simulator
from simulation.resultsloader import ResultsLoader
from util.loghelper import *

def GetAccessDBPathFromDir(dir):
    matchCount = 0
    match = ""
    for i in os.listdir(dir):
        if i.lower().endswith(".mdb"):
            matchCount += 1
            match = os.path.join(dir, i)
    if matchCount == 1:
        return match
    else: 
        raise AssertionError("expected a dir containing 1 access database, found {0}.  Directory='{1}'"
                             .format(matchCount, dir))

def get_base_projects(project_prefixes, base_project_path):
    return { x: GetAccessDBPathFromDir(os.path.join(base_project_path, x)) for x in project_prefixes }

def get_base_results(project_prefixes, base_project_path):
    return { x: GetAccessDBPathFromDir(os.path.join(base_project_path, x, "RESULTS")) for x in project_prefixes }

def copy_aidb_local(base_aidb_path, local_aidb_path):
    shutil.copy(base_aidb_path, local_aidb_path)

def copy_projects_local(project_prefixes, base_projects, local_working_dir):

    local_projects = {}
    #copy base projects local
    for p in project_prefixes:
        local_project_dir = os.path.join(local_working_dir, p)
        local_project_path = os.path.join(local_project_dir, "{0}_pre_dist_age.mdb".format(p))
        if os.path.exists(local_project_path):
            os.unlink(local_project_path)
        shutil.copy(base_projects[p], local_project_path)
        local_projects[p] = local_project_path

def simulate(local_project_path, local_aidb_path, cbm_exe_path):
    local_project = local_projects[project_prefix]
    with AIDB(local_aidb_path) as aidb, \
         AccessDB(local_project_path) as proj:
        aidb.DeleteProjectsFromAIDB()
        simId = aidb.AddProjectToAIDB(proj)
        s = Simulator(cbm_exe_path,
                      simId,
                      os.path.dirname(local_project_path),
                      r"C:\Program Files (x86)\Operational-Scale CBM-CFS3\temp",
                      r"C:\Program Files (x86)\Operational-Scale CBM-CFS3")
        s.simulate(False)
        r = ResultsLoader()
        r.loadResults(


def run():
    cbm_exe_path = r"M:\CBM Tools and Development\Builds\CBMBuilds\20180611_extended_kf5_passive_rule"
    base_aidb_path = r"M:\NIR_2019\03_Analysis\01_CBM\02_Production\02_SupplementaryData\01_CBMBugFixes\ArchiveIndex_NIR2019_CBMBugFixes.mdb"

    local_aidb_path = r"C:\Program Files (x86)\Operational-Scale CBM-CFS3\Admin\DBs\ArchiveIndex_Beta_Install.mdb"
    local_working_dir = r"c:\C:\pre_dist_age_run"
    base_project_path = r"\\dstore.pfc.forestry.ca\carbon1\NIR_2019\03_Analysis\01_CBM\02_Production\03_Scenarios\01_CBMBugFixes"
    project_prefixes = ["BCB","BCP","BCMN","BCMS","AB","SK","MB","ONW","ONE","QCG","QCL","QCR","NB","NS","PEI","NF","NWT","LB","YT","SKH","UF","AF"]
