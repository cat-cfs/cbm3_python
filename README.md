# cbm3_python

Python scripts for automating CBM-CFS3 tasks

## Requirements

* The [Operational-Scale CBM-CFS3 toolbox](https://www.nrcan.gc.ca/climate-change/impacts-adaptations/climate-change-impacts-forests/carbon-accounting/carbon-budget-model/13107)
* `cbm3_python` is windows only since the CBM-CFS3 model only operates in windows
* python 3x
* [git](https://git-scm.com/) - not strictly required but needed for the Standard Import tool workflow and also helpful for installing this package
* [python packages](https://github.com/cat-cfs/cbm3_python/blob/master/requirements.txt)



## Installing

The scripts can be directly installed from github

```bash
pip install git+https://github.com/cat-cfs/cbm3_python.git
```

## Workflows

### Importing CBM3 projects

cbm3_python has the ability to import CBM3 projects by calling the CBM Standard import tool as a library.

There are [example files with step by step instructions](./examples/sit_automation) for importing via command line, python script or jupyter notebook contained in the repository.


### Simulating CBM3 Projects

Simulation of a CBM3 project access database can be accomplished at the command line or via python script

#### command line

The following command will run an existing CBM-CFS3 project at path `my_proj.mdb` and create a results database at the path `\project\my_project_results.mdb`

```bash
cbm3_simulate \projects\my_cbm_project.mdb --results_database_path \projects\my_cbm_project_results.mdb
```

See [simulate.py](cbm3_python/scripts/simulate.py) for the full list of optional arguments for the cbm3_simulate command.

#### from python

```python
import cbm3_python.simulation.projectsimulator as projectsimulator

projectsimulator.simulate(
    project_path="/projects/my_cbm_project.mdb",
    results_database_path="/projects/my_cbm_project_results.mdb")
```

### Analysis of CBM3 Results Database

