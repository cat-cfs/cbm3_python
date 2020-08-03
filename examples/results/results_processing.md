---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.4.1
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

```python
from cbm3_python.simulation import projectsimulator
from cbm3_python.cbm3data import cbm3_results
```

```python
#using the bundled tutorial 3 project file for the purposes of this demonstration
project_path = "C:\Program Files (x86)\Operational-Scale CBM-CFS3\Tutorials\Tutorial 3\Tutorial3.mdb"
results_path = "C:\Program Files (x86)\Operational-Scale CBM-CFS3\Tutorials\Tutorial 3\Tutorial3_results.mdb"
results_path = projectsimulator.run(
    project_path=project_path,
    results_database_path=results_path)
```

The following lines load CBM3 results into pandas DataFrames from the loaded CBM3 results access database.

```python
pool_indicators = cbm3_results.load_pool_indicators(
    results_path, spatial_unit_grouping=True,
    classifier_set_grouping=True, land_class_grouping=True)
stock_changes = cbm3_results.load_stock_changes(
    results_path, spatial_unit_grouping=True,
    classifier_set_grouping=True, land_class_grouping=True)
disturbance_indicators = cbm3_results.load_disturbance_indicators(
    results_path, spatial_unit_grouping=True, disturbance_type_grouping=True,
    classifier_set_grouping=True, land_class_grouping=True)
age_indicators = cbm3_results.load_age_indicators(
    results_path, spatial_unit_grouping=True,
    classifier_set_grouping=True, land_class_grouping=True)
```

The following cells show a couple of examples of what can be done with the dataframes.

```python
# save the results to a csv file named tutorial3_pool_indicators.csv
pool_indicators.to_csv(
    r"C:\Program Files (x86)\Operational-Scale CBM-CFS3\Tutorials\Tutorial 3\tutorial3_pool_indicators.csv")
```

```python
# plot some results
pool_indicators[
    ['TimeStep','Total Biomass', 'Dead Organic Matter', 'Total Ecosystem']
].groupby('TimeStep').sum().plot(figsize=(10, 8))
```
