# CBM-CFS3 Standard import tool automation

The CBM-CFS3 Standard Import Tool (SIT) can be run using the cbm3_python package via bundled command line tools, or by calling cbm3_python as a library from a python script or Jupyter Notebook.

## Running from the Windows command line

The examples can be run using the cbm3_python built-in command `cbm3_sit_import`

The command's syntax is:

```
cbm3_sit_import <sit_data_dir> <output_filename>
```

`output_filename` is the path to the CBM-CFS3 project Access database created by importing the SIT dataset.

`sit_data_dir` points to a directory with the following standardized files:

* mapping.json
* sit_classifiers.csv
* sit_disturbance_types.csv
* sit_age_classes.csv
* sit_inventory.csv
* sit_yield.csv
* sit_events.csv
* sit_transitions.csv

The .csv files are in the tabular CBM Standard Import Tool format that is described in Chapter 3, section 3.1.1 of the [Operational-Scale CBM-CFS3 user guide](https://cfs.nrcan.gc.ca/pubwarehouse/pdfs/39768.pdf)

The format of the mapping.json file is described [here](https://github.com/cat-cfs/StandardImportToolPlugin/wiki/Mapping-Configuration).

## Running as a library

The python below can be used to import a SIT csv formatted dataset converting it to a CBM-CFS3 project Access database.

The pair of parameters required parameters are the same as those described in the *Running from the Windows command line* section above.

```python
from cbm3_python.cbm3data import sit_helper

sit_helper.csv_import(
    csv_dir=sit_data_dir, 
    imported_project_path=cbm3_project_path)
```

## Import with your own Archive Index database

If you have your own custom archive index database the SIT import process can target it. See the following command line or python script examples

**Windows command line with custom archive index**

Add the `--aidb_path` command line argument with the path to your custom archive index:

```bash
cbm3_sit_import <sit_data_dir> <output_filename> --aidb_path my_custom_archive_index.mdb
```

**Python script with custom archive index**

Specify the `archive_index_db_path` parameter in your call to the `csv_import` function

```python
from cbm3_python.cbm3data import sit_helper

sit_helper.csv_import(
    csv_dir=sit_data_dir, 
    imported_project_path=cbm3_project_path,
	archive_index_db_path="my_custom_archive_index.mdb")
```



## Initial set up steps

See the *requirements* section in the [docs](https://github.com/cat-cfs/cbm3_python/)

If cbm3_python is not already installed, it can be installed from this command:

```bash
pip install git+https://github.com/cat-cfs/cbm3_python
```



### Get the example files

Clone the repository to get a copy of the example files 

```bash
cd my_dir
git clone https://github.com/cat-cfs/cbm3_python
```

You now have a copy of cbm3_python sit_automation example files at `my_dir/cbm3_python/examples/sit_automation `



## Examples

### Administrative and Ecological classifier mapping

[/examples/admin_eco_classifier](./admin_eco_classifier) 

This example demonstrates a CBM-CFS3 SIT project that had data easily mapped to the CBM-CFS3 default administrative and ecological boundaries.

Run from the Windows command line

```bash
cbm3_sit_import my_dir/cbm3_python/examples/sit_automation/admin_eco_classifier my_project.mdb
```

This example corresponds with the CBM SIT user interface configuration in the image below.  Note that the 3<sup>rd</sup> radio button is checked.

![img](./img/admin_eco_classifiers.png)

### Spatial unit classifier mapping

[/examples/spatial_unit_classifier](./spatial_unit_classifier) 

The example above demonstrates a CBM-CFS3 SIT project that had data easily mapped to the CBM3 default spatial units.

Run from the Windows command line:

```bash
cbm3_sit_import my_dir/cbm3_python/examples/sit_automation/spatial_unit_classifier my_project.mdb
```

This example corresponds with the CBM SIT user interface configuration in the image below.  Note the 2<sup>nd</sup> radio button is checked.

![img](./img/spatial_classifier.png)

### No spatial unit classifier

[/examples/no_spatial_unit_classifier](./no_spatial_unit_classifier) 

This example demonstrates a CBM-CFS3 SIT project that was mapped to a single CBM-CFS3 default spatial unit

Run from the Windows command line:

```bash
cbm3_sit_import my_dir/cbm3_python/examples/sit_automation/no_spatial_unit_classifier my_project.mdb
```

This example corresponds with the CBM SIT user interface configuration in the image below.  Note the 1<sup>st</sup> radio button is checked.

![img](./img/no_spatial_classifier.png)
