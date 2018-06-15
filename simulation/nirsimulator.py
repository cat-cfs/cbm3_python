import os, shutil, logging
from cbm3data.accessdb import AccessDB
from cbm3data.aidb import AIDB
from cbm3data.access_templates import *
from simulation.simulator import Simulator
from simulation.resultsloader import ResultsLoader
from simulation.createaccountingrules import CreateAccountingRules
from simulation.rollup import Rollup

class NIRSimulator(object):

    def __init__(self, config):
        self.config = config

    def get_base_project_path(self, project_prefix):
        
        return self.GetAccessDBPathFromDir(
            os.path.join(self.config["base_project_dir"], project_prefix))

    def get_base_run_results_path(self, project_prefix, results_dir="RESULTS"):
        if project_prefix == "UF" or project_prefix == "AF":
            #special case: uf and af have nonstandard queries and 2 copies of RRDB
            return self.GetAccessDBPathFromDir(
                        os.path.join(self.config["base_project_dir"],
                        project_prefix, results_dir), newest = True)

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

    def run(self, prefix_filter = None):
        c = self.config

        self.copy_aidb_local(c["base_aidb_path"], c["local_aidb_path"])
        local_results_paths = []
        for p in c["project_prefixes"]:
            if not prefix_filter is None:
                if not p in prefix_filter:
                    continue
            logging.info("{}: Running CBM".format(p))
            self.copy_project_local(p)
            self.run_cbm(p)

            logging.info("{}: Load CBM Results".format(p))
            local_results_paths.append(self.load_project_results(p))

        result["local_rollup_path"] = os.path.join(c["local_working_dir"], c["local_rollup_filename"])
        logging.info("Rolling up CBM Results")
        self.do_rollup(
            rrdbs=local_results_paths,
            rollup_output_path= result["local_rollup_path"],
            local_aidb_path= c["local_aidb_path"])
        logging.info("CBM runs complete")

    def run_cbm(self, project_prefix):
        if project_prefix == "AF":
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
        with AIDB(local_aidb_path) as aidb, \
             AccessDB(local_project_path) as proj:
            aidb.DeleteProjectsFromAIDB()
            simId = aidb.AddProjectToAIDB(proj)
            cbm_wd = r"C:\Program Files (x86)\Operational-Scale CBM-CFS3\temp"
            s = Simulator(cbm_exe_path,
                          simId,
                          os.path.dirname(local_project_path),
                          cbm_wd,
                          r"C:\Program Files (x86)\Operational-Scale CBM-CFS3")
            s.CleanupRunDirectory()
            s.CopyToWorkingDir(os.path.join(ProjectDir,ProjectFileName))
            s.CreateCBMFiles()
            
            s.CopyCBMExecutable()
            s.DumpMakelistSVLs()
            s.RunCBM()
            s.LoadCBMResults()
            s.CopyTempFiles()

    def run_cbm_simulation(self, local_project_path, local_aidb_path,
                           cbm_exe_path, dist_classes_path, dist_rules_path):

        with AIDB(local_aidb_path) as aidb, \
             AccessDB(local_project_path) as proj:
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

    def GetAccessDBPathFromDir(self, dir, newest=False):
        matches = []
        for i in os.listdir(dir):
            if i.lower().endswith(".mdb"):
                matches.append(os.path.join(dir, i))
        if len(matches) == 1:
            return matches[0]
        elif len(matches) > 1 and newest:
            return sorted(matches, key= lambda x: os.path.getmtime(x), reverse=True)[0]
        elif len(matches) > 1 and not newest:
            raise AssertionError("found more than one access database.  Directory='{0}'".format(dir))
        else:
            raise AssertionError("expected a dir containing at least one access database, found {0}.  Directory='{1}'"
                                 .format(matchCount, dir))

    def copy_aidb_local(self, base_aidb_path, local_aidb_path):
        shutil.copy(base_aidb_path, local_aidb_path)

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

    def load_project_results(self, project_prefix):
        local_results_path =  self.get_local_results_path(project_prefix)
        local_project_path = self.get_local_project_path(project_prefix)
        local_aidb_path = self.config["local_aidb_path"]
        r = ResultsLoader()
        copy_rrdb_template(local_results_path)
        return r.loadResults(
            outputDBPath=local_results_path,
            aidbPath=local_aidb_path,
            projectDBPath=local_project_path,
            projectSimulationDirectory=r"C:\Program Files (x86)\Operational-Scale CBM-CFS3\temp",
            loadPreDistAge=True)

    def do_rollup(self, rrdbs, rollup_output_path, local_aidb_path):
        copy_rollup_template(rollup_output_path)
        r = Rollup(rrdbs, rollup_output_path, local_aidb_path)
        r.Roll()