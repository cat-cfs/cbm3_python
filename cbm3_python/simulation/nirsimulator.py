# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import os, shutil, logging, stat
from cbm3_python.cbm3data.accessdb import AccessDB
from cbm3_python.cbm3data.aidb import AIDB

from cbm3_python.cbm3data.rollup import Rollup

import cbm3_python.simulation.projectsimulator
from cbm3_python.simulation.tools.createaccountingrules import CreateAccountingRules
from cbm3_python.simulation.nir_sql.afforestation_fixes import *
from cbm3_python.simulation.nir_sql.unmanaged_forest_fixes import *

def run_af_simulation(local_project_path, local_aidb_path,
                      cbm_exe_path,
                      toolbox_path = r"C:\Program Files (x86)\Operational-Scale CBM-CFS3"):

    cbm3_python.simulation.projectsimulator.run(
        local_aidb_path, local_project_path, toolbox_path, cbm_exe_path,
        results_database_path=None, tempfiles_output_dir=None,
        skip_makelist=True, stdout_path=None, use_existing_makelist_output=False,
        dist_classes_path = None, dist_rules_path = None, 
        loader_settings={"type": "python_loader"})

def run_cbm_simulation(local_project_path, local_aidb_path, cbm_exe_path,
                       dist_classes_path, dist_rules_path, toolbox_path,
                       results_database_path, skip_makelist):

    cbm3_python.simulation.projectsimulator.run(
        local_aidb_path, local_project_path, toolbox_path, cbm_exe_path,
        results_database_path=None, tempfiles_output_dir=None,
        skip_makelist=skip_makelist, stdout_path=None, use_existing_makelist_output=False,
        dist_classes_path = dist_classes_path, dist_rules_path = dist_rules_path,
        loader_settings={"type": "python_loader"})


class NIRSimulator(object):

    def __init__(self, config, base_projects):
        self.config = config
        self.base_projects = { x["project_prefix"] : x for x in base_projects }

    def get_base_project_path(self, project_prefix):
        return self.base_projects[project_prefix]["project_path"]

    def get_base_run_results_path(self, project_prefix):
        return self.base_projects[project_prefix]["results_path"]

        return self.GetAccessDBPathFromDir(
            os.path.join(self.config["base_project_dir"],
                         project_prefix, results_dir))

    def get_local_project_dir(self, project_prefix):
        return os.path.join(self.config["local_working_dir"], project_prefix)

    def get_local_project_path(self, project_prefix):
        return os.path.join(
            self.get_local_project_dir(project_prefix),
            self.config["local_project_format"].format(project_prefix))

    def get_local_results_path(self, project_prefix):
        return os.path.join(
            self.get_local_project_dir(project_prefix),
            "results", self.config["local_results_format"].format(project_prefix))

    def get_local_rollup_db_path(self):
        return os.path.join(
            self.config["local_working_dir"], self.config["local_rollup_filename"])

    def run(self, prefix_filter = None):

        self.copy_aidb_local()
        local_results_paths = []
        for p in self.config["project_prefixes"]:
            if not prefix_filter is None:
                if not p in prefix_filter:
                    continue
            logging.info("{}: Running CBM".format(p))
            self.copy_project_local(p)
            self.run_cbm(p)

            logging.info("{}: Load CBM Results".format(p))
            rp = self.load_project_results(p)
            local_results_paths.append(rp)

        return local_results_paths

    def run_cbm(self, project_prefix):
        if project_prefix == "AF":

            prepare_afforestation_db(self.get_local_project_path(project_prefix),
                                     self.config["af_start_year"], self.config["af_end_year"])
            run_af_simulation(
                local_project_path = self.get_local_project_path(project_prefix),
                local_aidb_path=self.config["local_aidb_path"],
                cbm_exe_path= self.config["cbm_exe_path"])
            return

        run_cbm_simulation(
            local_project_path = self.get_local_project_path(project_prefix),
            local_aidb_path=self.config["local_aidb_path"],
            cbm_exe_path= self.config["cbm_exe_path"],
            dist_classes_path=self.config["dist_classes_path"],
            dist_rules_path=self.config["dist_rules_path"])



    def copy_aidb_local(self):
        logging.info("copying archive index - source: '{0}', dest: '{1}'"
                     .format(self.config["base_aidb_path"],
                             self.config["local_aidb_path"]))

        shutil.copy(self.config["base_aidb_path"],
                    self.config["local_aidb_path"])
        os.chmod(self.config["local_aidb_path"], stat.S_IWRITE)

    def copy_project_local(self, project_prefix):
        base_project_path = self.get_base_project_path(project_prefix)
        local_project_path = self.get_local_project_path(project_prefix)
        if os.path.exists(local_project_path):
            os.unlink(local_project_path)
        logging.info("copying project database source: '{0}', dest: '{1}'"
                         .format(base_project_path, local_project_path))
        if not os.path.exists(os.path.dirname(local_project_path)):
            os.makedirs(os.path.dirname(local_project_path))
        shutil.copy(base_project_path, local_project_path)
        os.chmod(local_project_path, stat.S_IWRITE)
        return local_project_path

    def load_project_results(self, project_prefix):
        local_results_path =  self.get_local_results_path(project_prefix)
        local_project_path = self.get_local_project_path(project_prefix)
        if project_prefix == "UF":
            run_uf_results_fixes(local_results_path)
        if project_prefix == "AF":
            run_af_results_fixes(local_results_path)
        return output

    def do_rollup(self, rrdbs):
        local_rollup_path = self.get_local_rollup_db_path()
        copy_rollup_template(local_rollup_path)
        r = Rollup(rrdbs, local_rollup_path, self.config["local_aidb_path"])
        r.Roll()