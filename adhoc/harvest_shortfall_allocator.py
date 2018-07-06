import csv, os
from analyse_report_fil import *

error_margin = 10 #percent
first_projection_year = 28

simdir = r"M:\2018_ETR\05_working\02_BAU\Simulation\2018-07-06 05_00_12"
report_fil = r"1\Tempfiles\CBMRun\output\report.fil"
mined_report_fil = r"1\Tempfiles\CBMRun\output\mined_report.fil.csv"
project_prefixes = ["BCB" ] #, "BCMN", "BCMS", "BCP"]

mined_report_fil_paths = {}
#0 mine report.fils
for p in project_prefixes:
    inpath = os.path.join(simdir, p, report_fil)
    outpath = os.path.join(simdir, p, mined_report_fil)
    with open(inpath, 'r') as infile, \
         open(outpath, 'w') as outfile:
        analyse_report_fil(
            inputFile=infile,
            outputFile=outfile,
            delimiter=",")
        mined_report_fil_paths[p] = outpath


data = []
#1. load the mined report.fil data
for k,v in mined_report_fil_paths.items():
    with open(v) as csvfile:
        reader = csv.DictReader(csvfile)
        project_data = list(reader)
        for row in project_data:
            row["project"] = k
        data.extend(project_data)


#1a. index rows by disturbance group and year, reject rows that dont have default dist type 4
grouped_data = {}
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


#2. identify disturbance groups with inadequate biomass for future harvest projection along with the first year at which the shortfall occurs
inadequate_dist_groups = []
inadequate_dist_group_keys = set([])
for k,v in grouped_data.items():
    timesteps = sorted(v.keys())
    for t in timesteps:
        if float(v[t]["Surplus Biomass C"]) < 1e-10 or float(v[t]["Biomass C Prop'n"]) > 1.0:
            inadequate_dist_groups.append({"disturbance_group": k, "shortfall_year": t})
            inadequate_dist_group_keys.add(k)
            break;

#3. compute the cumulative shortfall for each identified inadequate group
for d in inadequate_dist_groups:
    dist_group = d["disturbance_group"]
    timesteps = sorted([x for x in grouped_data[dist_group].keys() if x >= d["shortfall_year"]])
    cumulative_shorfall = 0
    for t in timesteps:
        cumulative_shorfall += float(grouped_data[dist_group][t]["Target Biomass C"])
    d["cumulative_shortfall"] = cumulative_shorfall

#4. identify the adequate groups by finding the final simulation point surplus for those disturbance groups that have remaining biomass
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


#5. reduce the inadequate group's harvest target by the cumulative shortfall + error margin (distributed evenly across projection years)
for d in inadequate_dist_groups:
    dist_group = d["disturbance_group"]
    harvest_shift = d["cumulative_shortfall"] * (1.0 + error_margin/100.0)
    
    projection_years = [year for year in sorted(grouped_data[dist_group].keys()) if year >= first_projection_year]
    total_projection_target = sum([float(grouped_data[dist_group][year]["Target Biomass C"]) for year in projection_years])
    #new harvest target is the sum of the projection target for all years less the reduction
    new_harvest_target = total_projection_target - harvest_shift
    harvest_proportion = new_harvest_target/total_projection_target
    d["harvest_shift"] = harvest_shift
    d["harvest_proportion"] = harvest_proportion

#6. allocate the amount reduced first weighted by the remaining biomass, then distributed across projection years to the adequate groups
adequate_dist_group_sum = sum([g["end_surplus"] for g in adequate_dist_groups])

for d_i in inadequate_dist_groups:
    for d_a in adequate_dist_groups:
        weight = d_a["end_surplus"] / adequate_dist_group_sum
        shift = d_i["harvest_shift"] * weight
        d_a["incoming_harvest_shifts"].append(shift)
    
#7. compute the new harvest proportions for the adequate groups
for d_a in adequate_dist_groups:
    new_target = d_a["total_projection_target"] + sum(d_a["incoming_harvest_shifts"])
    new_proportion = new_target / d_a["total_projection_target"]
    d_a["harvest_proportion"] = new_proportion

#8 write out the results
