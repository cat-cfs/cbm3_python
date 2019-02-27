# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import os, sys, shutil, argparse, json, logging, datetime
from cbm3_python.cbm3data.accessdb import AccessDB
from cbm3_python.simulation.nirsimulator import NIRSimulator
from cbm3_python.simulation.nirpathconfig import NIRPathConfig
from cbm3_python.simulation.nir_sql import nir_project_queries
from cbm3_python.simulation.nir_sql import hwpinput
from cbm3_python.simulation.tools.avgdisturbanceextender import AvgDisturbanceExtender
from cbm3_python.simulation.tools.disturbanceextender import DisturbanceExtender
from cbm3_python.simulation.tools.disturbancegeneratorconfig import DisturbanceGeneratorConfig
from cbm3_python.simulation.tools.disturbanceextension import DisturbanceExtension
from cbm3_python.simulation.tools import qaqc
from cbm3_python.util import loghelper


def load_json(path):
    with open(path, 'r') as f:
        return json.loads(f.read())
def get_date_stamp():
    return datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S")

def preprocess(config, project_prefix, project_path):
    with AccessDB(project_path, False) as nir_project_db:
        events_to_delete = config["EventsToDelete"]
        if len(events_to_delete)>0:
            sql_delete_events = nir_project_queries.sql_delete_disturbance_events(events_to_delete)
            nir_project_db.ExecuteQuery(query=sql_delete_events[0], params=sql_delete_events[1])

        post_delete_year = config["postDeleteYear"]
        sql_post_delete_year = nir_project_queries.sql_delete_post_year_events(post_delete_year)
        nir_project_db.ExecuteQuery(query=sql_post_delete_year[0], params=sql_post_delete_year[1])

        disturbance_extender_config = config["DisturbanceExtender"]
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

        disturbance_generator_config = config["DisturbanceGenerator"]
        if isinstance(disturbance_generator_config, dict):
            logging.info("running disturbance generator")
            disturbancegenerator = DisturbanceGeneratorConfig(
                os.path.abspath(disturbance_generator_config["ExePath"]),
                os.path.abspath(disturbance_generator_config["DefaultsPath"]),
                config["local_aidb_path"],
                disturbance_generator_config["Tasks"],
                project_prefix, project_path)
            disturbancegenerator.Run()
            logging.info("disturbance generator finished")
        else: logging.info("disturbance generator skipped")

        sql_run_length = nir_project_queries.sql_set_run_project_run_length(
            config["numTimeSteps"])
        nir_project_db.ExecuteQuery(query=sql_run_length[0],
                                   params=sql_run_length[1])
        nir_project_queries.run_simulation_id_cleanup(nir_project_db)
        nir_project_queries.update_random_seed(nir_project_db)


def main():
    try:
        parser = argparse.ArgumentParser(description="RunNation v2 script: "
                                         "processes and runs a batch of NIR "
                                         "simulations")
        parser.add_argument("--configuration", help="run nation configuration file")
        parser.add_argument("--nir_base_path_config", help="path to a csv file "
                            "containing the baseline project, and run results "
                            "database paths by project prefix")
        parser.add_argument("--local_working_dir", help="local working directory")
        parser.add_argument("--prefix_filter", help="optional comma delimited "
                            "prefixes, if included only the specified projects "
                            "will be included")
        parser.add_argument("--copy_local", action="store_true",
                           dest="copy_local", help="if present, copy the "
                           "projects and archive index to the local working "
                           "dir")
        parser.add_argument("--preprocess", action="store_true",
                           dest="preprocess", help="if present, run the "
                           "pre-processing steps on the local copies of "
                           "project databases")
        parser.add_argument("--simulate", action="store_true", dest="simulate",
                           help="if present, run the simulations for each of "
                           "the local copies of project databases")
        parser.add_argument("--rollup", action="store_true", dest="rollup",
                           help="if present, run the simulation rollup")
        parser.add_argument("--hwp_input", action="store_true",
                           dest="hwp_input", help="if specified hwp input is "
                           "generated")
        parser.add_argument("--qaqc", action="store_true", dest="qaqc",
                           help="if specified project level qaqc spreadsheets "
                           "are generated")
        parser.add_argument("--copy_to_final_results_dir", action="store_true",
                           dest="copy_to_final_results_dir", help="if present "
                           "results are copied to the final results dir (which "
                           "is specified in config). If unspecified no copy "
                           "will occur")

        args = parser.parse_args()

        date_stamp = get_date_stamp()

        config_path = os.path.abspath(args.configuration)
        config = load_json(args.configuration)
        base_path_config_file = os.path.abspath(args.nir_base_path_config)
        working_dir = os.path.abspath(args.local_working_dir)
        if not os.path.exists(working_dir):
            os.makedirs(working_dir)

        logpath = os.path.join(working_dir,
                 "{0}_{1}.log".format(date_stamp, config["Name"]))
        loghelper.start_logging(logpath, 'w+')

        #check that the user provided filter items actually exist in the config
        if args.prefix_filter:
            for x in args.prefix_filter.split(","):
                if not x in config["project_prefixes"]:
                    raise AssertionError(
                        "specified prefix filter item {} does not exist in "
                        "configuration project_prefixes".format(x))
        
        project_prefixes = config["project_prefixes"] \
            if not args.prefix_filter else \
            [x for x in config["project_prefixes"]
             if x in args.prefix_filter.split(",")]

        config["local_working_dir"] = working_dir

        base_path_config = NIRPathConfig()
        ns = NIRSimulator(config, base_path_config.load(base_path_config_file))

        if args.copy_local:
            logging.info("copying databases to working dir")
            ns.copy_aidb_local()
            for p in project_prefixes:
                ns.copy_project_local(p)

        if args.preprocess:
            for p in project_prefixes:
                if p == "AF":
                    continue
                logging.info("pre-processing {}".format(p))
                local_project_path = ns.get_local_project_path(p)
                preprocess(config, p, local_project_path)
                logging.info("finished pre-processing {}".format(p))

        if args.simulate:
            for p in project_prefixes:
                logging.info("simulating {}".format(p))
                ns.run_cbm(p)
                logging.info("finished simulating {}. Loading results".format(p))
                ns.load_project_results(p)
                logging.info("{} simulation finished".format(p))

        if args.rollup:
            rrdbs = [ns.get_local_results_path(p) for p in project_prefixes]
            logging.info("rolling up results dbs \n    {}".format("\n    ".join(rrdbs)))
            ns.do_rollup(rrdbs)

        if args.hwp_input:
            logging.info("generating hwp input in dir {}".format(working_dir))
            with AccessDB(ns.get_local_rollup_db_path()) as rollupDB:
                hwpinput.GeneratHWPInput(
                    rollupDB=rollupDB,
                    workingDir = working_dir)
            logging.info("generate hwp finished")

        if args.qaqc:
            logging.info("generating qaqc spreadsheets")

            for p in project_prefixes:
                local_dir = ns.get_local_project_dir(p)
                label = "{0}_{1}".format(p, config["Name"])
                qaqc.run_qaqc(
                    executable_path = config["QaqcExecutablePath"],
                    query_template_path = config["QaqcQueryTemplatePath"],
                    excel_template_path = config["QaqcExcelTemplatePath"],
                    excel_output_path = os.path.join(local_dir, "{}_qaqc.xlsx".format(label)),
                    work_sheet_tasks = qaqc.get_nir_worksheet_tasks(
                        RRDB_A_Label = "base_run",
                        RRDB_A_Path = ns.get_base_run_results_path(p),
                        RRDB_B_Label = label,
                        RRDB_B_Path = ns.get_local_results_path(p),
                        ProjectLabel = label,
                        ProjectPath = ns.get_local_project_path(p),
                        GWP_CH4=config["GWP_CH4"],
                        GWP_N2O=config["GWP_N2O"]))
            logging.info("generating qaqc spreadsheets finished")

        if args.copy_to_final_results_dir:
            
            final_results_dir = os.path.abspath(config["final_results_dir"])
            if not os.path.exists(final_results_dir):
                os.makedirs(final_results_dir)
            final_results_subdir = os.path.join(final_results_dir, date_stamp)
            logging.info("copying all contents of '{src}' to '{dest}'"
                         .format(src=working_dir, dest=final_results_subdir))
            shutil.copytree(working_dir, final_results_subdir)
            logging.info("finished copying")

    except Exception as ex:
        logging.exception("")


if __name__ == '__main__':
    main()