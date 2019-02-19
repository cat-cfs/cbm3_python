# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import os, shutil, logging, stat
from cbm3_python.cbm3data.accessdb import AccessDB
from cbm3_python.cbm3data.aidb import AIDB
from cbm3_python.cbm3data.access_templates import *
from cbm3_python.cbm3data.resultsloader import ResultsLoader
from cbm3_python.cbm3data.rollup import Rollup

from cbm3_python.simulation.simulator import Simulator
from cbm3_python.simulation.tools.createaccountingrules import CreateAccountingRules
from cbm3_python.simulation.nir_sql.afforestation_fixes import *
from cbm3_python.simulation.nir_sql.unmanaged_forest_fixes import *
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
            self.run_af_simulation( 
                local_project_path = self.get_local_project_path(project_prefix),
                local_aidb_path=self.config["local_aidb_path"],
                cbm_exe_path= self.config["cbm_exe_path"])
            return

        self.run_cbm_simulation(
            local_project_path = self.get_local_project_path(project_prefix),
            local_aidb_path=self.config["local_aidb_path"],
            cbm_exe_path= self.config["cbm_exe_path"],
            dist_classes_path=self.config["dist_classes_path"],
            dist_rules_path=self.config["dist_rules_path"])

    def run_af_simulation(self,local_project_path, local_aidb_path,
                           cbm_exe_path):
        with AIDB(local_aidb_path, False) as aidb, \
             AccessDB(local_project_path, False) as proj:
            aidb.DeleteProjectsFromAIDB()
            simId = aidb.AddProjectToAIDB(proj)
            cbm_wd = r"C:\Program Files (x86)\Operational-Scale CBM-CFS3\temp"
            s = Simulator(cbm_exe_path,
                          simId,
                          os.path.dirname(local_project_path),
                          cbm_wd,
                          r"C:\Program Files (x86)\Operational-Scale CBM-CFS3")
            s.CleanupRunDirectory()
            s.CopyToWorkingDir(local_project_path)
            s.CreateCBMFiles()
            
            s.CopyCBMExecutable()
            s.DumpMakelistSVLs()
            s.RunCBM()
            s.CopyTempFiles()

    def run_cbm_simulation(self, local_project_path, local_aidb_path,
                           cbm_exe_path, dist_classes_path, dist_rules_path):

        with AIDB(local_aidb_path, False) as aidb, \
             AccessDB(local_project_path, False) as proj:
            aidb.DeleteProjectsFromAIDB()
            simId = aidb.AddProjectToAIDB(proj)
            cbm_wd = r"C:\Program Files (x86)\Operational-Scale CBM-CFS3\temp"
            s = Simulator(cbm_exe_path,
                          simId,
                          os.path.dirname(local_project_path),
                          cbm_wd,
                          r"C:\Program Files (x86)\Operational-Scale CBM-CFS3")

            s.CleanupRunDirectory()
            s.CreateMakelistFiles()
            s.copyMakelist()
            s.runMakelist()
            s.loadMakelistSVLS()

            temp_proj_path = os.path.join(cbm_wd, os.path.basename(local_project_path))
            with AccessDB(temp_proj_path, False) as temp_proj:
                cr = CreateAccountingRules(temp_proj, dist_classes_path, dist_rules_path)
                cr.create_accounting_rules()

            s.copyMakelistOutput()
            s.CreateCBMFiles()
            s.CopyCBMExecutable()
            s.RunCBM()
            s.CopyTempFiles()

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
        local_aidb_path = self.config["local_aidb_path"]
        r = ResultsLoader()
        copy_rrdb_template(local_results_path)
        output = r.loadResults(
            outputDBPath=local_results_path,
            aidbPath=local_aidb_path,
            projectDBPath=local_project_path,
            projectSimulationDirectory=r"C:\Program Files (x86)\Operational-Scale CBM-CFS3\temp",
            loadPreDistAge=False)
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