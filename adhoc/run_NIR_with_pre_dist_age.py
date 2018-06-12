import os, shutil
from cbm3data.accessdb import AccessDB
from cbm3data.aidb import AIDB
from cbm3data.access_templates import *
from simulation.simulator import Simulator
from simulation.resultsloader import ResultsLoader
from simulation.createaccountingrules import CreateAccountingRules
from simulation.rollup import Rollup
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

def get_base_project_path( base_project_dir, project_prefix):
    return GetAccessDBPathFromDir(os.path.join(base_project_dir, project_prefix))

def get_base_results(base_project_dir, project_prefix):
    return GetAccessDBPathFromDir(os.path.join(base_project_dir, x, "RESULTS"))

def get_local_project_dir(local_working_dir, project_prefix):
    return os.path.join(local_working_dir, project_prefix)

def get_local_project_path(local_working_dir, project_prefix, local_name_format="{}.mdb"):
    return os.path.join(get_local_project_dir(local_working_dir, project_prefix),
                       local_name_format.format(project_prefix))

def get_local_results_path(local_working_dir, project_prefix, local_name_format="{}_results.mdb"):
    return os.path.join(get_local_project_dir(local_working_dir, project_prefix),
                        "results", local_name_format.format(project_prefix))

def copy_aidb_local(base_aidb_path, local_aidb_path):
    shutil.copy(base_aidb_path, local_aidb_path)

def copy_project_local(local_project_path, base_project_path):

    if os.path.exists(local_project_path):
        os.unlink(local_project_path)
    logging.info("copying project database source: '{0}', dest: '{1}'"
                     .format(base_project_path, local_project_path))
    if not os.path.exists(os.path.dirname(local_project_path)):
        os.makedirs(os.path.dirname(local_project_path))
    shutil.copy(base_project_path, local_project_path)

def simulate(local_project_path, local_aidb_path, cbm_exe_path,
             dist_classes_path, dist_rules_path):

    with AIDB(local_aidb_path) as aidb, \
         AccessDB(local_project_path) as proj:
        aidb.DeleteProjectsFromAIDB()
        simId = aidb.AddProjectToAIDB(proj)
        cbm_wd = r"C:\Program Files (x86)\Operational-Scale CBM-CFS3\temp"
        s = Simulator(cbm_exe_path,
                      simId,
                      os.path.dirname(local_project_path),
                      cbm_wd,
                      r"C:\Program Files (x86)\Operational-Scale CBM-CFS3")

        s.CleanupRunDirectory()
        s.CreateMakelistFiles()
        s.copyMakelist()
        s.runMakelist()
        s.loadMakelistSVLS()

        with AccessDB(os.path.join(cbm_wd, os.path.basename(local_project_path)), False) as temp_proj:
            cr = CreateAccountingRules(temp_proj, dist_classes_path, dist_rules_path)
            cr.create_accounting_rules()

        s.copyMakelistOutput()
        s.CreateCBMFiles()
        s.CopyCBMExecutable()
        s.RunCBM()

def load_project_results(results_path, local_aidb_path, local_project_path):
    r = ResultsLoader()
    copy_rrdb_template(results_path)
    return r.loadResults(
        outputDBPath=results_path,
        aidbPath=local_aidb_path,
        projectDBPath=local_project_path,
        projectSimulationDirectory=r"C:\Program Files (x86)\Operational-Scale CBM-CFS3\temp",
        loadPreDistAge=True)

def do_rollup(rrdbs, rollup_output_path, local_aidb_path):
    copy_rollup_template(rollup_output_path)
    r = Rollup(rrdbs, rollup_output_path, rollup_output_path, local_aidb_path)
    r.Roll()


def run():
    start_logging("run_nir_with_pre_dist_age.log")

    cbm_exe_path = r"M:\CBM Tools and Development\Builds\CBMBuilds\20180611_extended_kf5_passive_rule"
    base_aidb_path = r"M:\NIR_2019\03_Analysis\01_CBM\02_Production\02_SupplementaryData\01_CBMBugFixes\ArchiveIndex_NIR2019_CBMBugFixes.mdb"

    dist_rules_path = r"M:\NIR_2019\03_Analysis\01_CBM\01_Development\01_Scripts\08_NIR2017_NDExclusion_newrules_newexes\02a_disturbance_rules.csv"
    dist_classes_path = r"M:\NIR_2019\03_Analysis\01_CBM\01_Development\01_Scripts\08_NIR2017_NDExclusion_newrules_newexes\02b_disturbance_classes.csv"

    local_aidb_path = r"C:\Program Files (x86)\Operational-Scale CBM-CFS3\Admin\DBs\ArchiveIndex_Beta_Install.mdb"
    local_working_dir = r"C:\pre_dist_age_run"
    base_project_dir = r"\\dstore.pfc.forestry.ca\carbon1\NIR_2019\03_Analysis\01_CBM\02_Production\03_Scenarios\01_CBMBugFixes"

    project_prefixes = ["BCB","BCP","BCMN","BCMS","AB","SK","MB","ONW","ONE",
                        "QCG","QCL","QCR","NB","NS","PEI","NF","NWT","LB",
                        "YT","SKH","UF","AF"]

    copy_aidb_local(base_aidb_path, local_aidb_path)
    
    base_project_path = get_base_project_path(base_project_dir, "PEI")
    local_project_path = get_local_project_path(local_working_dir, "PEI", "{}_pre_dist_age.mdb")
    local_results_path = get_local_results_path(local_working_dir, "PEI", "{}_pre_dist_age_results.accdb")

    copy_project_local(
       local_project_path=local_project_path,
       base_project_path=base_project_path)

    simulate(
        local_project_path = local_project_path,
        local_aidb_path=local_aidb_path,
        cbm_exe_path=cbm_exe_path,
        dist_classes_path=dist_classes_path,
        dist_rules_path=dist_rules_path)

    load_project_results(local_results_path, local_aidb_path, local_project_path)

    do_rollup([local_results_path], os.path.join(local_working_dir, "rollup_db.accdb"), local_aidb_path)

run()