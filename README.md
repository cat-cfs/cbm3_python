# cbm3_python

Python scripts for automating CBM-CFS3 tasks

## Requirements

* The [CBM-CFS3 toolbox](https://www.nrcan.gc.ca/climate-change/impacts-adaptations/climate-change-impacts-forests/carbon-accounting/carbon-budget-model/13107)
* The Windows operating system
* python 3x
* [git](https://git-scm.com/) - not strictly required, but needed for the CBM Standard Import Tool workflow and also helpful for installing this package
* [python packages](https://github.com/cat-cfs/cbm3_python/blob/master/requirements.txt)



## Installing

The scripts can be directly installed from github

```bash
pip install git+https://github.com/cat-cfs/cbm3_python.git
```

## Workflows

### Importing CBM-CFS3 projects

cbm3_python has the ability to import CBM3-CFS3 projects by calling the CBM Standard import tool as a library.

[Example files with step by step instructions](./examples/sit_automation) for importing project data via Windows command line, Python script, or Jupyter Notebook contained in the repository.


### Simulating CBM-CFS3 Projects

Simulation of a CBM3 project access database can be accomplished at the command line or via python script

#### If using the Windows command line

The following command will run an existing CBM-CFS3 project at path `my_proj.mdb` and create a results database at the path `\project\my_project_results.mdb`

```bash
cbm3_simulate \projects\my_cbm_project.mdb --results_database_path \projects\my_cbm_project_results.mdb
```

See [simulate.py](cbm3_python/scripts/simulate.py) for the full list of optional arguments for the cbm3_simulate command.

#### If using a Python Script

```python
from cbm3_python.simulation import projectsimulator

projectsimulator.run(
    project_path="/projects/my_cbm_project.mdb",
    results_database_path="/projects/my_cbm_project_results.mdb")
```

### Analysis of CBM-CFS3 Results Database

cbm3_python has functions to assist results processing using [pandas](https://pandas.pydata.org/)

An example is included in notebook format, and markdown notebook format (using [jupytext](https://github.com/mwouts/jupytext))

* [ipynb](./examples/results/results_processing.ipynb)
* [jupytext markdown](./examples/results/results_processing.md)

