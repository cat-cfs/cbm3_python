#Copyright (C) Her Majesty the Queen in Right of Canada,
#as represented by the Minister of Natural Resources Canada

import os, subprocess, json, tempfile
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

def load_standard_import_tool_plugin():
    '''
    Download the 1.2.2.1 release of StandardImportToolPlugin from Github and unzip it locally.
    '''
    StandardImportToolPluginDir = os.path.join(".","StandardImportToolPlugin")
    #extra subdir in the archive
    StandardImportToolPluginExe = os.path.join(StandardImportToolPluginDir,"Release", "StandardImportToolPlugin.exe")
    if not os.path.exists(StandardImportToolPluginExe):
        resp = urlopen('https://github.com/cat-cfs/StandardImportToolPlugin/releases/download/1.2.2.1/Release.zip')
        zipfile = ZipFile(BytesIO(resp.read()))

        #os.makedirs(StandardImportToolPluginDir)
        zipfile.extractall(path=StandardImportToolPluginDir)
    return StandardImportToolPluginExe

class SITConfig(object):
    '''
    Class for working with standard import tool. Can be used to create configurations and import projects
    '''
    def __init__(self, imported_project_path, initialize_mapping=False, archive_index_db_path=None):
        self.config = {
            "output_path": imported_project_path,
            "mapping_config": {
                "initialize_mapping": initialize_mapping,
                "spatial_units": {
                    "mapping_mode": None,
                },
                "disturbance_types": None,
                "species": {
                    "species_classifier": None,
                },
                "nonforest": None
            }
        }
        if archive_index_db_path:
            self.config["archive_index_db_path"] = archive_index_db_path

    def save(self, path):
        dir = os.path.dirname(path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        with open(path, 'w') as configfile:
            configfile.write(json.dumps(self.config, indent=4))

    def import_project(self, exe_path, config_save_path=None):
        if config_save_path is None:
            with tempfile.NamedTemporaryFile(mode='w') as f:
                f.write(json.dumps(self.config))
                subprocess.check_call([exe_path,'-c', f.name])
        else:
            self.save(config_save_path)
            subprocess.check_call([exe_path,'-c', config_save_path])

    def set_species_classifier(self, name):
        self.config["mapping_config"]["species"]["species_classifier"] = name

    def set_single_spatial_unit(self, id):
        self.config["mapping_config"]["spatial_units"] = {}
        self.config["mapping_config"]["spatial_units"]["mapping_mode"] = "SingleDefaultSpatialUnit"
        self.config["mapping_config"]["spatial_units"]["default_spuid"] = id

    def set_admin_eco_mapping(self, admin_classifier, eco_classifier):
        self.config["mapping_config"]["spatial_units"] = {}
        self.config["mapping_config"]["spatial_units"]["mapping_mode"] = "SeperateAdminEcoClassifiers"
        self.config["mapping_config"]["spatial_units"]["admin_classifier"] = admin_classifier
        self.config["mapping_config"]["spatial_units"]["eco_classifier"] = eco_classifier

    def set_spatial_unit_mapping(self, spatial_unit_classifier):
        self.config["mapping_config"]["spatial_units"] = {}
        self.config["mapping_config"]["spatial_units"]["mapping_mode"] = "JoinedAdminEcoClassifier"
        self.config["mapping_config"]["spatial_units"]["spu_classifier"] = spatial_unit_classifier

    def set_non_forest_classifier(self, non_forest_classifier):
        self.config["mapping_config"]["nonforest"] = {}
        self.config["mapping_config"]["nonforest"]["nonforest_classifier"] = non_forest_classifier

    def map_disturbance_type(self, user, default):
        if self.config["mapping_config"]["disturbance_types"] is None:
            self.config["mapping_config"]["disturbance_types"] = {}
            self.config["mapping_config"]["disturbance_types"]["disturbance_type_mapping"] = []
        self.config["mapping_config"]["disturbance_types"]["disturbance_type_mapping"].append({
            "user_dist_type": user,
            "default_dist_type": default
        })

    def map_admin_boundary(self, user, default):
        if self.config["mapping_config"]["spatial_units"]["mapping_mode"] != "SeperateAdminEcoClassifiers":
            raise ValueError("cannot map admin boundary without admin/eco classifier mapping set")
        if not "admin_mapping" in self.config["mapping_config"]["spatial_units"]:
            self.config["mapping_config"]["spatial_units"]["admin_mapping"] = []
        self.config["mapping_config"]["spatial_units"]["admin_mapping"].append({
            "user_admin_boundary": user,
            "default_admin_boundary": default
        })

    def map_eco_boundary(self, user, default):
        if self.config["mapping_config"]["spatial_units"]["mapping_mode"] != "SeperateAdminEcoClassifiers":
            raise ValueError("cannot map eco boundary without admin/eco classifier mapping set")
        if not "eco_mapping" in self.config["mapping_config"]["spatial_units"]:
            self.config["mapping_config"]["spatial_units"]["eco_mapping"] = []
        self.config["mapping_config"]["spatial_units"]["eco_mapping"].append({
            "user_eco_boundary": user,
            "default_eco_boundary": default
        })

    def map_spatial_unit(self, user_spatial_unit, default_admin, default_eco):
        if self.config["mapping_config"]["spatial_units"]["mapping_mode"] != "JoinedAdminEcoClassifier":
            raise ValueError("cannot map spatial unit without spatial unit classifier mapping set")
        if not "spu_mapping" in self.config["mapping_config"]["spatial_units"]:
            self.config["mapping_config"]["spatial_units"]["spu_mapping"] = []
        self.config["mapping_config"]["spatial_units"]["spu_mapping"].append({
            "user_spatial_unit": user_spatial_unit,
            "default_spatial_unit": {
                "admin_boundary": default_admin,
                "default_eco": default_eco
            }
        })

    def map_species(self, user, default):
        if not "species_mapping" in self.config["mapping_config"]["species"]:
            self.config["mapping_config"]["species"]["species_mapping"] = []
        self.config["mapping_config"]["species"]["species_mapping"].append({
            "user_species": user,
            "default_species": default
        })

    def map_nonforest(self, user, default):
        if self.config["mapping_config"]["nonforest"] is None:
            raise ValueError("cannot map non forest value without non-forest classifier set")
        if not "nonforest_mapping" in self.config["mapping_config"]["nonforest"]:
            self.config["mapping_config"]["nonforest"]["nonforest_mapping"] = []
        self.config["mapping_config"]["nonforest"]["nonforest_mapping"].append({
            "user_nonforest_type": user,
            "default_nonforest_type": default
        })

    def text_file_paths(self, ageclass_path, classifiers_path,
        disturbance_events_path, disturbance_types_path, inventory_path,
        transition_rules_path, yield_path):
        if "import_config" in self.config or "data" in self.config:
            raise ValueError("only one call of function of text_file_paths, database_path, data_config may be used")

        self.config["import_config"] = {
            "ageclass_path": ageclass_path,
            "classifiers_path": classifiers_path,
            "disturbance_events_path": disturbance_events_path,
            "disturbance_types_path": disturbance_types_path,
            "inventory_path": inventory_path,
            "transition_rules_path": transition_rules_path,
            "yield_path": yield_path
        }

    def database_path(self, db_path, age_class_table_name, classifiers_table_name,
        disturbance_events_table_name, disturbance_types_table_name,
        inventory_table_name, transition_rules_table_name, yield_table_name):

        if "import_config" in self.config or "data" in self.config:
            raise ValueError("only one call of function of text_file_paths, database_path, data_config may be used")

        self.config["import_config"] = {
            "path": db_path,
            "ageclass_table_name": age_class_table_name,
            "classifiers_table_name": classifiers_table_name,
            "disturbance_events_table_name": disturbance_events_table_name,
            "disturbance_types_table_name": disturbance_types_table_name,
            "inventory_table_name": inventory_table_name,
            "transition_rules_table_name": transition_rules_table_name,
            "yield_table_name": yield_table_name
        }

    def data_config(self, age_class_size, num_age_classes, classifiers):

        if "import_config" in self.config or "data" in self.config:
            raise ValueError("only one call of function of text_file_paths, database_path, data_config may be used")

        self.config["data"] = {
            "age_class": {"age_class_size":age_class_size, "num_age_classes":num_age_classes},
            "classifiers": classifiers,
            "disturbance_events": [],
            "inventory": [],
            "transition_rules": [],
            "yield": []
        }

    def add_event(self, **kwargs):
        self.config["data"]["disturbance_events"].append(kwargs)

    def add_inventory(self, **kwargs):
        self.config["data"]["inventory"].append(kwargs)

    def add_transition_rule(self, **kwargs):
        self.config["data"]["transition_rules"].append(kwargs)

    def add_yield(self, **kwargs):
        self.config["data"]["yield"].append(kwargs)
