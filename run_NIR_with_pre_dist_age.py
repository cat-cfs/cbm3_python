from util.loghelper import *

from cbm3data.accessdb import AccessDB
from simulation.nirsimulator import NIRSimulator
import os, csv
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.stats.weightstats import DescrStatsW

start_logging("run_nir_with_pre_dist_age.log")

def query_to_np_matrix(db_path, query):
    with AccessDB(db_path) as db:
        res = db.Query(query).fetchall()
        if len(res) > 0:
            np_result = np.array([[float(y) for y in x] for x in res], dtype=np.float)
            return np_result
        return None

def compare_disturbance_areas(base_rrdb_path, local_rrdb_path, project_prefix, outputdir):
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)
    # compares disturbance areas between the new pre-dist 
    # age indicator and base dist indicators to check for bugs

    local_sql = """
        SELECT tblPreDistAge.TimeStep, Sum(tblPreDistAge.AreaDisturbed) AS SumOfAreaDisturbed
        FROM tblPreDistAge GROUP BY tblPreDistAge.TimeStep;"""

    base_sql = """
        SELECT tblDistIndicators.TimeStep, Sum(tblDistIndicators.DistArea) AS SumOfDistArea
        FROM tblDistIndicators 
        WHERE tblDistIndicators.DistTypeID <> 0
        GROUP BY tblDistIndicators.TimeStep
        ORDER BY tblDistIndicators.TimeStep;"""

    f, arr = plt.subplots(2, 1)
    f.set_figwidth(11)
    f.set_figheight(7)

    local_np_res = query_to_np_matrix(local_rrdb_path, local_sql)
    base_np_res = query_to_np_matrix(base_rrdb_path, base_sql)
    arr[0].set_title("{} Disturbance Areas Validation: Comparison between distindicators, and predistage indicator".format(project_prefix))
    arr[0].set_ylabel("Area [ha]")
    arr[0].plot(local_np_res[:,0], local_np_res[:,1], linestyle='--')
    arr[0].plot(base_np_res[:,0], base_np_res[:,1], linestyle=':')
    rel_dif = np.abs((local_np_res - base_np_res) / (local_np_res + base_np_res)/2)
    rel_dif[:,0] = local_np_res[:,0]
    arr[1].set_title("{} Disturbance Areas Validation: Differences".format(project_prefix))
    arr[1].set_ylabel("Area Relative Differences [ha]")
    arr[1].plot(rel_dif[:,0], rel_dif[:,1])

    arr[0].legend(["pre_dist_age_indicator", "dist_indicators"], loc="upper left") 
    plt.tight_layout()
    plt.savefig(os.path.join(outputdir, "disturbances_comparison_{}.png".format(project_prefix)))
    plt.close("all")

    np.savetxt(fname=os.path.join(outputdir, "disturbances_comparison_{}.csv".format(project_prefix)),
               X=np.column_stack((local_np_res, base_np_res[:,1], rel_dif[:,1])),
               header="timestep,predistage_area,distindicator_area,relative_difference",
               delimiter=",")

def load_wildfire_disturbance_rules(disturbance_rules_path):
    disturbance_rules = {}
    with open('disturbance_rules_path', 'rb') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row["disturbance_class"] == "Wildfire":
                disturbance_rules[int(row["defaultSPUID"])] = int(row["rule_value"])
    return disturbance_rules

def plot_project_level_pre_dist_age(local_rrdb_path, project_prefix, outputdir):
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)

    sql = """
        SELECT tblSPU.DefaultSPUID, tblPreDistAge.TimeStep, tblPreDistAge.PreDisturbanceAge, tblPreDistAge.AreaDisturbed
        FROM (tblPreDistAge INNER JOIN tblSPU ON tblPreDistAge.SPUID = tblSPU.SPUID)
        INNER JOIN tblDisturbanceType ON tblPreDistAge.DistTypeID = tblDisturbanceType.DistTypeID
        WHERE tblDisturbanceType.DefaultDistTypeID = 1;"""

    data = query_to_np_matrix(local_rrdb_path, sql)



config = {
    "cbm_exe_path": r"M:\CBM Tools and Development\Builds\CBMBuilds\20180611_extended_kf5_passive_rule",
    "base_aidb_path": r"M:\NIR_2019\03_Analysis\01_CBM\03_Production\02_SupplementaryData\01_CBMBugFixes\ArchiveIndex_NIR2019_CBMBugFixes.mdb",
    "base_project_dir": r"M:\NIR_2019\03_Analysis\01_CBM\03_Production\03_Scenarios\01_CBMBugFixes",
    "dist_rules_path": r"M:\NIR_2019\03_Analysis\01_CBM\01_Development\01_Scripts\08_NIR2017_NDExclusion_newrules_newexes\02a_disturbance_rules.csv",
    "dist_classes_path": r"M:\NIR_2019\03_Analysis\01_CBM\01_Development\01_Scripts\08_NIR2017_NDExclusion_newrules_newexes\02b_disturbance_classes.csv",

    "local_aidb_path": r"C:\Program Files (x86)\Operational-Scale CBM-CFS3\Admin\DBs\ArchiveIndex_Beta_Install.mdb",
    "local_working_dir": r"C:\pre_dist_age_run",
    "local_project_format": "{}_pre_dist_age.mdb",
    "local_results_format": "{}_pre_dist_age_results.accdb",
    "local_rollup_filename": "rollup_db.accdb",

    "project_prefixes": ["BCB","BCP","BCMN","BCMS","AB","SK","MB","ONW","ONE","QCG","QCL","QCR","NB","NS","PEI","NF","NWT","LB","YT","SKH","UF","AF"]
}

n = NIRSimulator(config)
#n.load_project_results("MB")
#n.run(prefix_filter = ["ONW","ONE","QCG","QCL","QCR","NB","NS","PEI","NF","NWT","LB","YT","SKH","UF"])
for p in ["BCB","BCP","BCMN","BCMS","AB","SK","MB", "PEI"]:
    base_rrdb_path = n.get_base_run_results_path(project_prefix)
    local_rrdb_path = n.get_local_results_path(project_prefix)
    #compare_disturbance_areas(
    #    base_rrdb_path = base_rrdb_path,
    #    local_rrdb_path = local_rrdb_path,
    #    project_prefix = p,
    #    outputdir=os.path.join(
    #        config["local_working_dir"],
    #        "validation",
    #        "disturbance_areas")
    plot_project_level_pre_dist_age(
        local_rrdb_path=local_rrdb_path,
        project_prefix=p,
        outputdir=os.path.join(
            config["local_working_dir"],
            "validation",
            "project_level_pre_dist_age")