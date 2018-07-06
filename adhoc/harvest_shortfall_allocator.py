import csv

mined_report_fil_path = r"C:\dev\bcb_mined_report.csv"

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
        if year in grouped_data[disturbance_group]:
            grouped_data[disturbance_group][year].append(row)
        else:
            grouped_data[disturbance_group] = {year: row}
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
    timesteps = sorted([x for x in grouped_data[d].keys() if x >= d["shortfall_year"]])
    cumulative_shorfall = 0
    for t in timesteps:
        cumulative_shorfall += float(grouped_data[d][t]["Target Biomass C"])
    d["cumulative_shorfall"] = cumulative_shorfall

#4. identify the adequate groups by finding the final simulation point surplus for those disturbance groups that have remaining biomass

#5. reduce the inadequate group's harvest target by the cumulative shortfall (distributed evenly across projection years)

#6. allocate the amount reduced first weighted by the remaining biomass, then distributed across projection years to the adequate groups

