from util.loghelper import *
from cbm3data.accessdb import AccessDB
from simulation.nirsimulator import NIRSimulator
import numpy as np
import matplotlib.pyplot as plt

start_logging("run_nir_with_pre_dist_age.log")

def compare_disturbance_areas(n, project_prefix, outputdir):
    # compares disturbance areas between the new pre-dist 
    # age indicator and base dist indicators to check for bugs
    base_rrdb_path = n.get_base_run_results_path(project_prefix)
    local_rrdb_path = n.get_local_results_path(project_prefix)

    local_sql = """
        SELECT tblPreDistAge.TimeStep, tblPreDistAge.DistTypeID, Sum(tblPreDistAge.AreaDisturbed) AS SumOfAreaDisturbed
        FROM tblPreDistAge GROUP BY tblPreDistAge.TimeStep, tblPreDistAge.DistTypeID;"""

    base_sql = """
        SELECT tblDistIndicators.TimeStep, tblDistIndicators.DistTypeID, Sum(tblDistIndicators.DistArea) AS SumOfDistArea
        FROM tblDistIndicators GROUP BY tblDistIndicators.TimeStep, tblDistIndicators.DistTypeID
        HAVING tblDistIndicators.DistTypeID)<>0
        ORDER BY tblDistIndicators.TimeStep, tblDistIndicators.DistTypeID;"""

    f, arr = plt.subplots(2, 1)
    f.set_figwidth(15)
    f.set_figheight(15)
    with AccessDB(local_rrdb_path) as local_rrdb:
        res = local_rrdb.Query(local_sql).fetchall()
        np_result = np.array([[float(y) for y in x] for x in res], dtype=np.float)

        arr[0].set_title("PEI Disturbance Areas")
        arr[0].set_ylabel("Area [ha]")
        unique_dist_types = np.unique(np_result[:,1])
        for u in unique_dist_types:
            plot_rows = np_result[np.where(np_result[:,1] == u)]
            arr[0].plot(plot_rows[:,0], plot_rows[:,2])


    plt.legend(["dist type {}".format(int(x)) for x in unique_dist_types], loc="upper left") 
    plt.tight_layout()
    plt.show()##"test.tif")
    plt.close("all")




config = {
    "cbm_exe_path": r"M:\CBM Tools and Development\Builds\CBMBuilds\20180611_extended_kf5_passive_rule",
    "base_aidb_path": r"M:\NIR_2019\03_Analysis\01_CBM\02_Production\02_SupplementaryData\01_CBMBugFixes\ArchiveIndex_NIR2019_CBMBugFixes.mdb",
    "base_project_dir": r"\\dstore.pfc.forestry.ca\carbon1\NIR_2019\03_Analysis\01_CBM\03_Production\03_Scenarios\01_CBMBugFixes",
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
n.load_project_results("MB")
#n.run()
compare_disturbance_areas(n, "PEI", "out")