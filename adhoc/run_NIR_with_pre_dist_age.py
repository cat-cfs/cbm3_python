import sys
sys.path.append('../')

from util.loghelper import *

from cbm3data.accessdb import AccessDB
from simulation.nirsimulator import NIRSimulator
import os, csv
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.stats.weightstats import DescrStatsW

start_logging("run_nir_with_pre_dist_age.log")

def query_to_np_matrix(db_path, query, params=None):
    with AccessDB(db_path) as db:
        res = db.Query(query,params).fetchall()
        if len(res) > 0:
            np_result = np.array([[float(y) for y in x] for x in res], dtype=np.float)
            return np_result
        return None

def qaqc_comparison_plot_wide_data(data1, data2, legend_labels, plot_title, xlabel, ylabel, out_img_path):
    """
    plot a comparison between 2 numeric tables of data
    with columns:
    x, y0, y1, ..., yn

    The pair of tables must have equivalent columns and rows

    Creates 2 plots 
    1: the value of each y column with x as the x axis

    2: the relative difference of the corresponding y columns for each table
    """

    if data1.shape != data2.shape:
        raise AssertionError("data1 and data2 must have equivalent shape")
    n_y = data1.shape[1] - 1 
    rel_dif = np.abs((data1 - data2) / (data1 + data2)/2)
    rel_dif[:,0] = data1[:,0]
    f, arr = plt.subplots(2, 1)
    f.set_figwidth(11)
    f.set_figheight(7)
    
    arr[0].set_title(plot_title)
    arr[1].set_title("{} relative differences".format(plot_title))
    arr[0].set_ylabel(ylabel)
    arr[1].set_ylabel("relative differences")
    arr[0].set_xlabel(xlabel)
    arr[1].set_xlabel(xlabel)
    for y in range(0,n_y):
        col_idx = y+1

        arr[0].plot(data1[:,0], data1[:,col_idx], linestyle='--')
        arr[0].plot(data2[:,0], data2[:,col_idx], linestyle=':')
        arr[1].plot(rel_dif[:,0], rel_dif[:,col_idx])

    arr[0].legend(legend_labels, loc="upper left") 
    plt.tight_layout()
    plt.savefig(out_img_path)
    plt.close("all")
#   

#def qaqc_comparison_plot_long_data(data1, data2):
#    if data1.shape != data2.shape:
#        raise AssertionError

def compare_forest_areas(base_rrdb_path, local_rrdb_path, project_prefix, outputdir):
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)
    # compares disturbance areas between the new pre-dist 
    # age indicator and base dist indicators to check for bugs

    sql = """
        SELECT tblAgeIndicators.TimeStep, Sum(tblAgeIndicators.Area) AS SumOfArea, tblAgeIndicators.LandClassID
        FROM tblAgeIndicators
        GROUP BY tblAgeIndicators.TimeStep, tblAgeIndicators.LandClassID
        HAVING (((tblAgeIndicators.LandClassID)=0));
        """

    local_np_res = query_to_np_matrix(local_rrdb_path, sql)
    base_np_res = query_to_np_matrix(base_rrdb_path, sql)
    rel_dif = np.abs((local_np_res - base_np_res) / (local_np_res + base_np_res)/2)
    rel_dif[:,0] = local_np_res[:,0]
    np.savetxt(fname=os.path.join(outputdir, "forest_area_comparison_{}.csv".format(project_prefix)),
               X=np.column_stack((local_np_res, base_np_res[:,1], rel_dif[:,1])),
               header="timestep,predistage_area,distindicator_area,relative_difference",
               delimiter=",")
    f, arr = plt.subplots(2, 1)
    f.set_figwidth(11)
    f.set_figheight(7)
    arr[0].set_title("{} Forest Areas Validation: Comparison between base run, and local run".format(project_prefix))
    arr[0].set_ylabel("Area [ha]")
    arr[0].plot(local_np_res[:,0], local_np_res[:,1], linestyle='--')
    arr[0].plot(base_np_res[:,0], base_np_res[:,1], linestyle=':')
    arr[1].set_title("{} Forest Areas Validation: Differences".format(project_prefix))
    arr[1].set_ylabel("Area Relative Differences [ha]")
    arr[1].plot(rel_dif[:,0], rel_dif[:,1])

    arr[0].legend(["local_run", "base_run"], loc="upper left") 
    plt.tight_layout()
    plt.savefig(os.path.join(outputdir, "forest_area_comparison_{}.png".format(project_prefix)))
    plt.close("all")

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
    with open(disturbance_rules_path, 'rb') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row["disturbance_class"] == "Wildfire":
                disturbance_rules[int(row["defaultSPUID"])] = int(row["rule_value"])
    return disturbance_rules

def plot_national_level_pre_dist_age(rollup_path, outputdir):
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)

    sql = "SELECT tblPreDistAge.PreDisturbanceAge, tblPreDistAge.AreaDisturbed FROM tblPreDistAge where tblPreDistAge.DistTypeID = 1;"
    data_national = query_to_np_matrix(rollup_path, sql)
    national_stats = DescrStatsW(data = data_national[:,0], weights=data_national[:,1])
    with open(os.path.join(outputdir, "national_level.csv"), 'w') as csvfile:
        fieldnames = ["weighted_mean_age", "n_events", "max_age", "min_age", "weighted_std_dev", "weighted_variance"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, lineterminator='\n')
        writer.writeheader()
        writer.writerow({
            "weighted_mean_age": national_stats.mean,
            "n_events": national_stats.data.size, 
            "max_age": national_stats.data.max(),
            "min_age": national_stats.data.min(),
            "weighted_std_dev": national_stats.std,
             "weighted_variance": national_stats.var})

    sql_admin = "SELECT tblAdminBoundaryDefault.AdminBoundaryID, tblAdminBoundaryDefault.AdminBoundaryName FROM tblAdminBoundaryDefault;"
    sql_eco = "SELECT tblEcoboundaryDefault.EcoBoundaryID, tblEcoboundaryDefault.EcoBoundaryName FROM tblEcoboundaryDefault;" 
    sql_spu = """
        SELECT tblSPUDefault.SPUID, tblAdminBoundaryDefault.AdminBoundaryName, tblEcoboundaryDefault.EcoBoundaryName
        FROM (tblSPUDefault INNER JOIN tblAdminBoundaryDefault ON tblSPUDefault.AdminBoundaryID = tblAdminBoundaryDefault.AdminBoundaryID) INNER JOIN tblEcoboundaryDefault ON tblSPUDefault.EcoBoundaryID = tblEcoboundaryDefault.EcoBoundaryID
        ORDER BY tblSPUDefault.SPUID;"""

    with AccessDB(rollup_path) as rrdb:
        admin_boundaries = list(rrdb.Query(sql_admin))
        eco_boundaries = list(rrdb.Query(sql_eco))
        spus = list(rrdb.Query(sql_spu))

    with open(os.path.join(outputdir, "national_level_by_admin.csv"), 'w') as csvfile:
        fieldnames = ["admin_boundary", "weighted_mean_age", "n_events", "max_age", "min_age", "weighted_std_dev", "weighted_variance"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, lineterminator='\n')
        writer.writeheader()
        for admin in admin_boundaries:
            admin_filter_sql = """
                SELECT tblSPUDefault.AdminBoundaryID, tblPreDistAge.PreDisturbanceAge, tblPreDistAge.AreaDisturbed
                FROM tblPreDistAge INNER JOIN tblSPUDefault ON tblPreDistAge.SPUID = tblSPUDefault.SPUID
                WHERE tblSPUDefault.AdminBoundaryID=? and tblPreDistAge.DistTypeID = 1;
            """
            data_admin = query_to_np_matrix(rollup_path, admin_filter_sql, (admin.AdminBoundaryID,))
            if data_admin is None:
                continue
            national_admin_stats = DescrStatsW(data = data_admin[:,1], weights=data_admin[:,2])
            writer.writerow({
                "admin_boundary": admin.AdminBoundaryName,
                "weighted_mean_age": national_admin_stats.mean,
                "n_events": national_admin_stats.data.size, 
                "max_age": national_admin_stats.data.max(),
                "min_age": national_admin_stats.data.min(),
                "weighted_std_dev": national_admin_stats.std,
                 "weighted_variance": national_admin_stats.var})

    with open(os.path.join(outputdir, "national_level_by_eco.csv"), 'w') as csvfile:
        fieldnames = ["eco_boundary", "weighted_mean_age", "n_events", "max_age", "min_age", "weighted_std_dev", "weighted_variance"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, lineterminator='\n')
        writer.writeheader()
        for eco in eco_boundaries:
            eco_filter_sql = """
                SELECT tblSPUDefault.EcoBoundaryID, tblPreDistAge.PreDisturbanceAge, tblPreDistAge.AreaDisturbed
                FROM tblPreDistAge INNER JOIN tblSPUDefault ON tblPreDistAge.SPUID = tblSPUDefault.SPUID
                WHERE tblSPUDefault.EcoBoundaryID=? and tblPreDistAge.DistTypeID = 1;
                """
            data_eco = query_to_np_matrix(rollup_path, eco_filter_sql, (eco.EcoBoundaryID,))
            if data_eco is None:
                continue
            national_eco_stats = DescrStatsW(data = data_eco[:,1], weights=data_eco[:,2])
            writer.writerow({
                "eco_boundary": eco.EcoBoundaryName,
                "weighted_mean_age": national_eco_stats.mean,
                "n_events": national_eco_stats.data.size, 
                "max_age": national_eco_stats.data.max(),
                "min_age": national_eco_stats.data.min(),
                "weighted_std_dev": national_eco_stats.std,
                 "weighted_variance": national_eco_stats.var})

    with open(os.path.join(outputdir, "national_level_by_spu.csv"), 'w') as csvfile:
        fieldnames = ["reporting_unit_id", "admin_boundary", "eco_boundary", "weighted_mean_age", "n_events", "max_age", "min_age", "weighted_std_dev", "weighted_variance"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, lineterminator='\n')
        writer.writeheader()
        for spu in spus:
            spu_filter_sql = """
                SELECT tblSPUDefault.SPUID, tblPreDistAge.PreDisturbanceAge, tblPreDistAge.AreaDisturbed
                FROM tblPreDistAge INNER JOIN tblSPUDefault ON tblPreDistAge.SPUID = tblSPUDefault.SPUID
                WHERE tblSPUDefault.SPUID=? and tblPreDistAge.DistTypeID = 1;
                """
            data_spu = query_to_np_matrix(rollup_path, spu_filter_sql, (spu.SPUID,))
            if data_spu is None:
                continue
            national_spu_stats = DescrStatsW(data = data_spu[:,1], weights=data_spu[:,2])
            writer.writerow({
                "reporting_unit_id": spu.SPUID,
                "admin_boundary": spu.AdminBoundaryName,
                "eco_boundary": spu.EcoBoundaryName,
                "weighted_mean_age": national_spu_stats.mean,
                "n_events": national_spu_stats.data.size, 
                "max_age": national_spu_stats.data.max(),
                "min_age": national_spu_stats.data.min(),
                "weighted_std_dev": national_spu_stats.std,
                 "weighted_variance": national_spu_stats.var})



def plot_project_level_pre_dist_age(local_rrdb_path, project_prefix, dist_rules, outputdir):
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)

    sql = """
        SELECT tblSPU.DefaultSPUID, tblPreDistAge.TimeStep, tblPreDistAge.PreDisturbanceAge, tblPreDistAge.AreaDisturbed
        FROM (tblPreDistAge INNER JOIN tblSPU ON tblPreDistAge.SPUID = tblSPU.SPUID)
        INNER JOIN tblDisturbanceType ON tblPreDistAge.DistTypeID = tblDisturbanceType.DistTypeID
        WHERE tblDisturbanceType.DefaultDistTypeID = 1;"""

    data = query_to_np_matrix(local_rrdb_path, sql)
    if data is None:
        return
    unique_default_spu = np.unique(data[:,0])
    unique_default_spu_timesteps = np.unique(data[:,[0,1]], axis=0)
    by_spu_stats = np.zeros(shape=(len(unique_default_spu), 8))
    by_spu_timestep_stats = np.zeros(shape=(len(unique_default_spu_timesteps),9))

    for i,u in enumerate(unique_default_spu):
        rows = data[np.where(data[:,0]==u)]
        by_spu_stats_row = DescrStatsW(data=rows[:,2],weights=rows[:,3])
        by_spu_stats[i,:]=np.array(
            [
                u, 
                dist_rules[u],
                by_spu_stats_row.mean,
                by_spu_stats_row.data.size,
                by_spu_stats_row.data.max(),
                by_spu_stats_row.data.min(),
                by_spu_stats_row.std,
                by_spu_stats_row.var
            ],
           dtype=np.float)

    for i,u in enumerate(unique_default_spu_timesteps):
        rows = data[np.where((data[:,0]==u[0]) & (data[:,1]==u[1]))]
        by_timestep_stats_row = DescrStatsW(data=rows[:,2],weights=rows[:,3])

        by_spu_timestep_stats[i,:] = np.array(
            [
                u[0],
                u[1],
                dist_rules[u[0]],
                by_timestep_stats_row.mean,
                by_timestep_stats_row.data.size,
                by_timestep_stats_row.data.max(),
                by_timestep_stats_row.data.min(),
                by_timestep_stats_row.std,
                by_timestep_stats_row.var
            ],
           dtype=np.float)


    np.savetxt(fname=os.path.join(outputdir, "wildfire_pre_dist_ages_spu_{}.csv".format(project_prefix)),
               X=by_spu_stats,
               header="defaultSPU,reentry_rule_value,area_weighted_mean_age,n_disturbances,max_age,min_age,std_age,var_age",
               delimiter=",")

    np.savetxt(fname=os.path.join(outputdir, "wildfire_pre_dist_ages_spu_timestep_{}.csv".format(project_prefix)),
               X=by_spu_timestep_stats,
               header="defaultSPU,timestep,reentry_rule_value,area_weighted_mean_age,n_disturbances,max_age,min_age,std_age,var_age",
               delimiter=",")

    plt.figure(figsize=(15,12))

    legend = []
    markers = ['v','^','<','>','1','2','3','4','s','p', '*']
    colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k', 'w']
    for i, u in enumerate(unique_default_spu):
        rows = by_spu_timestep_stats[np.where(by_spu_timestep_stats[:,0]==u)]
        plt.plot(rows[:,1], rows[:,2], color=colors[i], marker=markers[i], markerfacecolor='None')
        plt.plot(rows[:,1], rows[:,3], color=colors[i], marker=markers[i], markerfacecolor='None', linestyle = 'None')
        legend.append("re-entry rule SPU {}".format(int(u)))
        legend.append("area weighted mean age at fire SPU {}".format(int(u)))
    

    plt.title("NIR Project '{}' pre dist area weighted mean age by year for multiple default SPUs".format(project_prefix))
    plt.ylabel("Area weighted mean age [years]")
    plt.xlabel("Year")
    plt.legend(legend, loc="upper left") 
    plt.tight_layout()
    plt.savefig(os.path.join(outputdir, "pre_dist_age_by_year_{}.png".format(project_prefix)))
    plt.close("all")

    plt.figure(figsize=(15,12))
    plt.plot(by_spu_stats[:,0], by_spu_stats[:,1], marker='*', markerfacecolor='None', linestyle = 'None')
    plt.plot(by_spu_stats[:,0], by_spu_stats[:,2], marker='*', markerfacecolor='None', linestyle = 'None')
    legend.append("re-entry rule values".format(int(u)))
    legend.append("area weighted mean age".format(int(u)))

    plt.title("NIR Project '{}' pre dist area weighted mean age by default SPU".format(project_prefix))
    plt.ylabel("Area weighted mean age, or re-entry rule value [years]")
    plt.xlabel("Spatial Unit ID")
    plt.legend(legend, loc="upper left")
    plt.tight_layout()
    plt.savefig(os.path.join(outputdir, "pre_dist_age_by_spu_{}.png".format(project_prefix)))
    plt.close("all")

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

    "af_start_year": 1970,
    "af_end_year": 2016,
    "project_prefixes": ["BCB","BCP","BCMN","BCMS","AB","SK","MB","ONW","ONE","QCG","QCL","QCR","NB","NS","PEI","NF","NWT","LB","YT","SKH","UF","AF"]
}

n = NIRSimulator(config)
#n.run(prefix_filter = ["UF"])
rollup_path = os.path.join(config["local_working_dir"],"pre_dist_age_rollup.mdb")

#n.do_rollup(
#    rrdbs = [n.get_local_results_path(x) for x in config["project_prefixes"]],
#    rollup_output_path = rollup_path,
#    local_aidb_path= config["local_aidb_path"])

for p in ["AF"]: #config["project_prefixes"]:
    base_rrdb_path = n.get_base_run_results_path(p)
    local_rrdb_path = n.get_local_results_path(p)
    plot_national_level_pre_dist_age(
        rollup_path= rollup_path,
        outputdir=os.path.join(
            config["local_working_dir"],
            "national_level_pre_dist_age"))
    #compare_forest_areas(
    #    base_rrdb_path = base_rrdb_path,
    #    local_rrdb_path = local_rrdb_path,
    #    project_prefix = p,
    #    outputdir=os.path.join(
    #        config["local_working_dir"],
    #        "validation",
    #        "forest_areas"))
    #
    #compare_disturbance_areas(
    #    base_rrdb_path = base_rrdb_path,
    #    local_rrdb_path = local_rrdb_path,
    #    project_prefix = p,
    #    outputdir=os.path.join(
    #        config["local_working_dir"],
    #        "validation",
    #        "disturbance_areas"))
    #
    #plot_project_level_pre_dist_age(
    #    local_rrdb_path=local_rrdb_path,
    #    project_prefix=p,
    #    dist_rules = load_wildfire_disturbance_rules(config["dist_rules_path"]),
    #    outputdir=os.path.join(
    #        config["local_working_dir"],
    #        "project_level_pre_dist_age"))