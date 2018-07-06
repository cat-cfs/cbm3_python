import csv

mined_report_fil_path = r"C:\dev\bcb_mined_report.csv"
error_margin = 20 #percent
first_projection_year = 28

#1. load the mined report.fil data
with open(mined_report_fil_path) as csvfile:
    reader = csv.DictReader(csvfile)
    data = list(reader)
    #for row in reader:
    #    print(row['first_name'], row['last_name'])

#1a. index rows by disturbance group and year, reject rows that dont have default dist type 4
grouped_data = {}
for row in data:
    default_dist_type = int(row["Default Disturbance Type"])
    if default_dist_type != 4:
        continue
    disturbance_group = int(row["Disturbance Group"])
    year = int(row["Year"])
    if disturbance_group in grouped_data:
        grouped_data[disturbance_group][year] = row
    else:
        grouped_data[disturbance_group] = {year: row}


#2. identify disturbance groups with inadequate biomass for future harvest projection along with the first year at which the shortfall occurs
inadequate_dist_groups = []
for k,v in grouped_data.items():
    timesteps = sorted(v.keys())
    for t in timesteps:
        if float(v[t]["Surplus Biomass C"]) < 1e-8 or float(v[t]["Biomass C Prop'n"]) > 1.0:
            inadequate_dist_groups.append({"disturbance_group": k, "shortfall_year": t})
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
    if k in inadequate_dist_groups:
        continue
    last_timestep = sorted(v.keys())[-1]
    adequate_dist_groups.append({"disturbance_group": k, "end_surplus": float(v[t]["Surplus Biomass C"])})

#5. reduce the inadequate group's harvest target by the cumulative shortfall + error margin (distributed evenly across projection years)
for d in inadequate_dist_groups:
    dist_group = d["disturbance_group"]
    reduction = d["cumulative_shortfall"] * (1.0 + error_margin/100.0)
    timesteps = sorted([x for x in grouped_data[dist_group].keys()
                        if x >= max(d["shortfall_year"], first_projection_year)])
    per_timestep_reduction = reduction / len(timesteps)


#6. allocate the amount reduced first weighted by the remaining biomass, then distributed across projection years to the adequate groups

