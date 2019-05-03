
import os, shutil, json, logging
from cbm3_python.cbm3data.accessdb import AccessDB
from cbm3_python.simulation.nirsimulator import NIRSimulator
import cbm3_python.simulation.nirpathconfig as nirpathconfig
from cbm3_python.simulation.nir_sql import nir_project_queries
from cbm3_python.simulation.nir_sql import hwpinput
from cbm3_python.simulation.tools.avgdisturbanceextender import AvgDisturbanceExtender
from cbm3_python.simulation.tools.disturbanceextender import DisturbanceExtender
from cbm3_python.simulation.tools.disturbancegeneratorconfig import DisturbanceGeneratorConfig
from cbm3_python.simulation.tools.disturbanceextension import DisturbanceExtension
import cbm3_python.simulation.tools.qaqc
from cbm3_python.simulation.tools.dgchecker import DGChecker

def load_json(path):
    with open(path, 'r') as f:
        return json.loads(f.read())

class ETRSimulator():

    def __init__(self, config_path, base_path_config_file, local_working_dir):
        self.config = load_json(config_path)
        self.local_working_dir = local_working_dir
        
        if not os.path.exists(local_working_dir):
            os.makedirs(local_working_dir)

        self.config["local_working_dir"] = local_working_dir
        self.ns = NIRSimulator(self.config, nirpathconfig.load(base_path_config_file))

        self.local_tools_dir = os.path.join(local_working_dir, "tools")
        
        disturbance_generator_config = self.config["DisturbanceGenerator"]
        if isinstance(disturbance_generator_config, dict):  
            self.dg_checker =  DGChecker(
                self.config["DisturbanceGenerator"]["DefaultsPath"],
                self.config["DisturbanceGenerator"]["Tasks"])

    def load_project_prefixes(self, prefix_filter):
        if prefix_filter:
            #check that the user provided filter items actually exist in the config
            for x in prefix_filter.split(","):
                if not x in self.config["project_prefixes"]:
                    raise AssertionError(
                        "specified prefix filter item {} does not exist in "
                        "configuration project_prefixes".format(x))

        project_prefixes = self.config["project_prefixes"] \
            if not prefix_filter else \
            [x for x in self.config["project_prefixes"]
                if x in prefix_filter.split(",")]
        return project_prefixes


    def copy_tool_local(self, original_path):
        local_dir = os.path.join(
            self.local_tools_dir,
            os.path.splitext(
                os.path.basename(original_path))[0])
        local_path = os.path.join(
            local_dir,
            os.path.basename(original_path))
        if not os.path.exists(local_dir):
            logging.info("copying {0} to {1}".format(original_path,local_dir))
            shutil.copytree(src=os.path.dirname(original_path),
                            dst=local_dir)
        return local_path


    def run_disturbance_extender(self, project_path):
        with AccessDB(project_path, False) as nir_project_db:
            events_to_delete = self.config["EventsToDelete"]
            if len(events_to_delete) > 0:
                sql_delete_events = nir_project_queries.sql_delete_disturbance_events(events_to_delete)
                nir_project_db.ExecuteQuery(query=sql_delete_events[0], params=sql_delete_events[1])

            post_delete_year = self.config["postDeleteYear"]
            sql_post_delete_year = nir_project_queries.sql_delete_post_year_events(post_delete_year)
            nir_project_db.ExecuteQuery(query=sql_post_delete_year[0], params=sql_post_delete_year[1])

            disturbance_extender_config = self.config["DisturbanceExtender"]
            if isinstance(disturbance_extender_config, list):
                avgDisturbanceExtender = AvgDisturbanceExtender()
                simpleDisturbanceExtender = DisturbanceExtender()
                logging.info("running disturbance extender")
                for mode, extender in (("Simple", simpleDisturbanceExtender),
                                        ("Avg", avgDisturbanceExtender)):
                    disturbanceExtensions = [DisturbanceExtension(
                            extension["Name"],
                            defaultDistTypeIDs=extension["DisturbanceTypeIDs"],
                            fromYear=extension["FromYear"],
                            toYear=extension["ToYear"])
                        for extension in disturbance_extender_config
                        if extension["Mode"] == mode]
                    extender.Run(nir_project_db, disturbanceExtensions)
                logging.info("disturbance extender finished")
            else: logging.info("disturbance extender skipped")

    def run_disturbance_generator(self, project_prefix, project_path):
        disturbance_generator_config = self.config["DisturbanceGenerator"]
        if isinstance(disturbance_generator_config, dict):
            logging.info("running disturbance generator")

            dg_path = self.copy_tool_local(disturbance_generator_config["ExePath"])

            disturbancegenerator = DisturbanceGeneratorConfig(
                os.path.abspath(dg_path),
                os.path.abspath(disturbance_generator_config["DefaultsPath"]),
                self.config["local_aidb_path"],
                disturbance_generator_config["Tasks"],
                project_prefix, project_path)
            disturbancegenerator.Run()
            logging.info("disturbance generator finished")
        else: logging.info("disturbance generator skipped")


    def check_disturbance_generator_output(self, project_prefix, project_path):
        disturbance_generator_config = self.config["DisturbanceGenerator"]
        if isinstance(disturbance_generator_config, dict):
            logging.info("checking disturbance generator output for {0}".format(project_prefix))
            self.dg_checker.check(project_prefix, project_path)
        else:logging.info("no disturbance event to check")


    def run_nir_sql(self, project_path):
        with AccessDB(project_path, False) as nir_project_db:
            sql_run_length = nir_project_queries.sql_set_run_project_run_length(
                self.config["numTimeSteps"])
            nir_project_db.ExecuteQuery(query=sql_run_length[0],
                                       params=sql_run_length[1])
            nir_project_queries.run_simulation_id_cleanup(nir_project_db)
            nir_project_queries.update_random_seed(nir_project_db)

    def run_nir_sql_af(self, project_path):
        with AccessDB(project_path, False) as nir_project_db:
            sql_run_length = nir_project_queries.sql_set_run_project_run_length(
                self.config["af_end_year"]-self.config["af_start_year"] + 1)
            nir_project_db.ExecuteQuery(query=sql_run_length[0],
                                       params=sql_run_length[1])
            nir_project_queries.run_simulation_id_cleanup(nir_project_db)

    def run(self, prefix_filter, copy_local, preprocess, simulate, rollup, hwp_input, 
            qaqc, copy_to_final_results_dir, date_stamp):

        project_prefixes = self.load_project_prefixes(prefix_filter)

        if copy_local:
            logging.info("copying databases to working dir")
            self.ns.copy_aidb_local()
            for p in project_prefixes:
                self.ns.copy_project_local(p)

            logging.info("copying tools to local working dir")


        if preprocess:
            for p in project_prefixes:
                logging.info("pre-processing {}".format(p))
                local_project_path = self.ns.get_local_project_path(p)
                if p == "AF":
                    self.run_nir_sql_af(local_project_path)
                else:
                    self.run_disturbance_extender(local_project_path)
                    self.run_disturbance_generator(p,local_project_path)
                    self.check_disturbance_generator_output(p,local_project_path)
                    self.run_nir_sql(local_project_path)
                logging.info("finished pre-processing {}".format(p))

        if simulate:
            for p in project_prefixes:
                logging.info("simulating {}".format(p))
                self.ns.run_cbm(p)
                logging.info("finished simulating {}. processing results".format(p))
                self.ns.run_results_post_processing(p)
                logging.info("{} simulation finished".format(p))

        if rollup:
            rrdbs = [self.ns.get_local_results_path(p) for p in project_prefixes]
            logging.info("rolling up results dbs \n    {}".format("\n    ".join(rrdbs)))
            self.ns.do_rollup(rrdbs)

        if hwp_input:
            logging.info("generating hwp input in dir {}".format(self.local_working_dir))
            with AccessDB(self.ns.get_local_rollup_db_path()) as rollupDB:
                hwpinput.GeneratHWPInput(
                    rollupDB=rollupDB,
                    workingDir = self.local_working_dir)
            logging.info("generate hwp finished")

        if qaqc:
            logging.info("generating qaqc spreadsheets")

            for p in project_prefixes:
                local_dir = self.ns.get_local_project_dir(p)
                label = "{0}_{1}".format(p, self.config["Name"])
                cbm3_python.simulation.tools.qaqc.run_qaqc(
                    executable_path = self.copy_tool_local(self.config["QaqcExecutablePath"]),
                    query_template_path = self.config["QaqcQueryTemplatePath"],
                    excel_template_path = self.config["QaqcExcelTemplatePath"],
                    excel_output_path = os.path.join(local_dir, "{}_qaqc.xlsx".format(label)),
                    work_sheet_tasks = cbm3_python.simulation.tools.qaqc.get_nir_worksheet_tasks(
                        RRDB_A_Label = "base_run",
                        RRDB_A_Path = self.ns.get_base_run_results_path(p),
                        RRDB_B_Label = label,
                        RRDB_B_Path = self.ns.get_local_results_path(p),
                        ProjectLabel = label,
                        ProjectPath = self.ns.get_local_project_path(p),
                        GWP_CH4=self.config["GWP_CH4"],
                        GWP_N2O=self.config["GWP_N2O"]))
            logging.info("generating qaqc spreadsheets finished")

        if copy_to_final_results_dir:

            final_results_dir = os.path.abspath(self.config["final_results_dir"])
            if not os.path.exists(final_results_dir):
                os.makedirs(final_results_dir)
            final_results_subdir = os.path.join(final_results_dir, date_stamp)
            logging.info("copying all contents of '{src}' to '{dest}'"
                         .format(src=self.local_working_dir, dest=final_results_subdir))
            shutil.copytree(self.local_working_dir, final_results_subdir)
            logging.info("finished copying")
