# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import csv, os, sys, json, argparse, datetime, logging
sys.path.append('../')
from cbm3_python.adhoc.analyse_report_fil import *
from cbm3_python.util import loghelper



def load_json(path):
    with open(path, 'r') as f:
        return json.loads(f.read())

def mine_report_fils(config, project_prefixes, do_mining):
    mined_report_fil_paths = {}
    for p in project_prefixes:
        inpath = config["report_fil_path_format"].format(project_prefix=p)
        outpath =  config["mined_report_fil_path_format"].format(project_prefix=p)

        if do_mining:
            with open(inpath, 'r') as infile, \
                 open(outpath, 'w') as outfile:

                logging.info("{0}: reading cbm report.fil '{1}'".format(p, inpath))
                analyse_report_fil(
                    inputFile=infile,
                    outputFile=outfile,
                    delimiter=",")

        mined_report_fil_paths[p] = outpath
    return mined_report_fil_paths

def load_report_fil_data(mined_report_fil_paths):
    data = []

    for k,v in mined_report_fil_paths.items():
        with open(v) as csvfile:
            logging.info("{0}: loading cbm report.fil '{1}'".format(k, v))
            reader = csv.DictReader(csvfile)
            project_data = list(reader)
            for row in project_data:
                row["project"] = k
            data.extend(project_data)
    return data

def index_rows(data):
    grouped_data = {}
    logging.info("indexing rows")
    for row in data:
        default_dist_type = int(row["Default Disturbance Type"])
        if default_dist_type != 4:
            continue
        disturbance_group = (row["project"],int(row["Disturbance Group"])) #composite key (project,disturbance group)
        year = int(row["Year"])
        if disturbance_group in grouped_data:
            grouped_data[disturbance_group][year] = row
        else:
            grouped_data[disturbance_group] = {year: row}
    return grouped_data

def identify_inadequate_disturbance_groups(grouped_data):

    inadequate_dist_groups = []
    inadequate_dist_group_keys = set([])
    for k,v in grouped_data.items():
        timesteps = sorted(v.keys())
        for t in timesteps:
            if float(v[t]["Surplus Biomass C"]) < 1e-10 or float(v[t]["Biomass C Prop'n"]) > 1.0:
                inadequate_dist_groups.append({"disturbance_group": k, "shortfall_year": t})
                inadequate_dist_group_keys.add(k)
                logging.info("project {0} disturbance group {1} has harvest shortfall".format(k[0],k[1]))
                break;
    return inadequate_dist_group_keys, inadequate_dist_groups

def compute_cumulative_shortfalls(inadequate_dist_groups, grouped_data):
    for d in inadequate_dist_groups:
        dist_group = d["disturbance_group"]
        timesteps = sorted([x for x in grouped_data[dist_group].keys() if x >= d["shortfall_year"]])
        cumulative_shorfall = 0
        for t in timesteps:
            target = float(grouped_data[dist_group][t]["Target Biomass C"])
            surplus = grouped_data[dist_group][t]["Surplus Biomass C"]
            bioCProp = grouped_data[dist_group][t]["Biomass C Prop'n"]
            if float(surplus) < 1e-10 or float(bioCProp) > 1.0: #filter out years that have surplus
                cumulative_shorfall += target
        logging.info("project {0} disturbance group {1} cumulative shortfall: {2}"
                     .format(dist_group[0],dist_group[1],cumulative_shorfall))
        d["cumulative_shortfall"] = cumulative_shorfall

def identify_adequate_groups(first_projection_year, grouped_data, inadequate_dist_group_keys):
    adequate_dist_groups = []
    for k,v in grouped_data.items():
        if k in inadequate_dist_group_keys:
            continue
        last_timestep = sorted(v.keys())[-1]
        d_g = {"disturbance_group": k, "end_surplus": float(v[last_timestep]["Surplus Biomass C"])}
        projection_years = [year for year in sorted(v.keys()) if year >= first_projection_year]
        d_g["total_projection_target"] = sum([float(v[x]["Target Biomass C"]) for x in projection_years])
        d_g["incoming_harvest_shifts"] = []
        adequate_dist_groups.append(d_g)
        logging.info("project {0} disturbance group {1} biomass surplus: {2}"
                     .format(k[0],k[1],d_g["end_surplus"]))
    return adequate_dist_groups

def reduce_harvest_targets(inadequate_dist_groups, error_margin, first_projection_year, grouped_data, disturbance_group_spugroup_map):

    summary_file_path = os.path.abspath("harvest_reduction_summary.csv")
    logging.info("harvest reduction summary file {}".format(summary_file_path))
    with open(summary_file_path, 'wb') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["project", "spugroup", "cumulative_shortfall", "harvest_shift", "total_projection_target", "new_projection_target", "proportion"])
        for d in inadequate_dist_groups:
        
            dist_group = d["disturbance_group"]
            spu_group =  disturbance_group_spugroup_map[dist_group[0]][str(dist_group[1])]
            logging.info("Computing reduced harvest. Project: {}, Spugroup {}".format(
                dist_group[0],spu_group))

            harvest_shift = d["cumulative_shortfall"] * (1.0 + error_margin/100.0)
    
            projection_years = [year for year in sorted(grouped_data[dist_group].keys()) if year >= first_projection_year]
            total_projection_target = sum([float(grouped_data[dist_group][year]["Target Biomass C"]) for year in projection_years])
            
            #cap the harvest target shift at total projection target
            harvest_shift = total_projection_target if harvest_shift > total_projection_target else harvest_shift
            
            #new harvest target is the sum of the projection target for all years less the reduction
            new_harvest_target = total_projection_target - harvest_shift
            harvest_proportion = new_harvest_target/total_projection_target

            d["harvest_shift"] = harvest_shift
            d["harvest_proportion"] = harvest_proportion
            writer.writerow([dist_group[0],spu_group, d["cumulative_shortfall"], harvest_shift, total_projection_target, new_harvest_target, harvest_proportion])


def allocate_reduced_harvest(inadequate_dist_groups, adequate_dist_groups, disturbance_group_spugroup_map):
    adequate_dist_group_sum = sum([g["end_surplus"] for g in adequate_dist_groups])

    logging.info("allocating reduced harvest among groups that have surplus")
    summary_file_path = os.path.abspath("harvest_reallocation_summary.csv")
    logging.info("harvest reallocation summary file {}".format(summary_file_path))
    with open(summary_file_path, 'wb') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["dest_project", "dest_spugroup", "end_surplus", "final_surplus_weight", "src_project", "src_spugroup", "harvest_C_shift" ])
        for d_i in inadequate_dist_groups:
            for d_a in adequate_dist_groups:
                weight = d_a["end_surplus"] / adequate_dist_group_sum
                shift = d_i["harvest_shift"] * weight
                d_a["incoming_harvest_shifts"].append(shift)

                dest_project = d_a["disturbance_group"][0]
                dest_spugroup = disturbance_group_spugroup_map[dest_project][str(d_a["disturbance_group"][1])]
                end_surplus  = d_a["end_surplus"]
                final_surplus_weight = weight
                src_project = d_i["disturbance_group"][0]
                src_spugroup = disturbance_group_spugroup_map[src_project][str(d_i["disturbance_group"][1])]
                harvest_C_shift = shift
                writer.writerow([
                    dest_project,
                    dest_spugroup,
                    end_surplus,
                    final_surplus_weight,
                    src_project,
                    src_spugroup,
                    harvest_C_shift
                    ])

def compute_new_harvest_proportions(adequate_dist_groups, disturbance_group_spugroup_map):
    logging.info("computing new harvest proportions for adequate groups")
    summary_file_path = os.path.abspath("new_harvest_proportions_summary.csv")
    logging.info("adequate groups new harvest proportions summary file {}".format(summary_file_path))
    with open(summary_file_path, 'wb') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["project", "spugroup", "total_projection_target", "total_incoming_harvest_shifts", "new_target", "new_proportion" ])

        for d_a in adequate_dist_groups:
            new_target = d_a["total_projection_target"] + sum(d_a["incoming_harvest_shifts"])
            new_proportion = new_target / d_a["total_projection_target"]
            d_a["harvest_proportion"] = new_proportion

            project = d_a["disturbance_group"][0]
            spugroup = disturbance_group_spugroup_map[project][str(d_a["disturbance_group"][1])]
            total_projection_target = d_a["total_projection_target"]
            total_incoming_harvest_shifts = sum(d_a["incoming_harvest_shifts"])

            writer.writerow([project, spugroup, total_projection_target, total_incoming_harvest_shifts, new_target, new_proportion])



def write_results(adequate_dist_groups, inadequate_dist_groups, output_file_path, disturbance_group_spugroup_map):
    with open(output_file_path, 'wb') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["project","spugroupid","shifted_harvest_proportion"])
        get_project_prefix = lambda x: x["disturbance_group"][0]
        def get_spugroup(x):
            project_prefix = get_project_prefix(x)
            project_map = disturbance_group_spugroup_map[project_prefix]
            report_fil_disturbance_group = str(x["disturbance_group"][1])
            if report_fil_disturbance_group not in project_map:
                raise ValueError("specified report fil disturbacne group not in project '{}' map. Disturbance group {}"
                                 .format(project_prefix, report_fil_disturbance_group))
            cbm_spugroup = project_map[report_fil_disturbance_group]
            return cbm_spugroup

        get_harvest_proportion = lambda x: x["harvest_proportion"]
    
        rows = [
            [
                get_project_prefix(x),
                get_spugroup(x),
                get_harvest_proportion(x)
            ] for x in adequate_dist_groups]
        rows.extend([
            [
                get_project_prefix(x),
                get_spugroup(x),
                get_harvest_proportion(x)
            ] for x in inadequate_dist_groups])
        rows.sort(key=lambda x: (x[0],x[1]))
        for r in rows:
            writer.writerow(r)

def run_harvest_reallocator(config_path, do_mining=True):
    config = load_json(config_path)
    error_margin = config["error_margin_percent"]
    first_projection_year = config["first_projection_year"]

    project_prefixes = config["project_prefixes"]
    output_file_path = config["output_file_path"]
    mined_report_fil_paths = {}

    disturbance_group_spugroup_map = config["disturbance_group_spugroup_map"]


    #0 mine report.fils
    mined_report_fil_paths = mine_report_fils(config, project_prefixes, do_mining)

    #1. load the mined report.fil data
    data = load_report_fil_data(mined_report_fil_paths)

    #1a. index rows by disturbance group and year, reject rows that dont have default dist type 4
    grouped_data = index_rows(data)

    #2. identify disturbance groups with inadequate biomass for future harvest projection along with the first year at which the shortfall occurs
    inadequate_dist_group_keys, inadequate_dist_groups = identify_inadequate_disturbance_groups(grouped_data)

    #3. compute the cumulative shortfall for each identified inadequate group
    compute_cumulative_shortfalls(inadequate_dist_groups, grouped_data)

    #4. identify the adequate groups by finding the final simulation point surplus for those disturbance groups that have remaining biomass
    adequate_dist_groups = identify_adequate_groups(first_projection_year, grouped_data, inadequate_dist_group_keys)

    #5. reduce the inadequate group's harvest target by the cumulative shortfall + error margin (distributed evenly across projection years)
    reduce_harvest_targets(inadequate_dist_groups, error_margin, first_projection_year, grouped_data, disturbance_group_spugroup_map)

    #6. allocate the amount reduced first weighted by the remaining biomass, then distributed across projection years to the adequate groups
    allocate_reduced_harvest(inadequate_dist_groups, adequate_dist_groups, disturbance_group_spugroup_map)
    
    #7. compute the new harvest proportions for the adequate groups
    compute_new_harvest_proportions(adequate_dist_groups, disturbance_group_spugroup_map)

    #8 write out the results
    write_results(adequate_dist_groups, inadequate_dist_groups, output_file_path, disturbance_group_spugroup_map)

    

def main():
    try:
        parser = argparse.ArgumentParser(description="projected harvest reallocator")
        parser.add_argument("--configuration", help="path to json config file")
        parser.add_argument("--mine_report_fil", action="store_true",
                            dest="mine_report_fil", help="if true mine report.fil files specified in config")
        args = parser.parse_args()
        
        logpath = os.path.join( os.getcwd(), "harvest_shortfall_allocator_{}.log".format(
            datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S")))
        loghelper.start_logging(logpath, 'w+')

        run_harvest_reallocator(args.configuration, args.mine_report_fil)

    except Exception as ex:
        logging.exception("")
if __name__ == '__main__':
    main()