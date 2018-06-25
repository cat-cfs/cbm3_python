import os, sys, shutil, argparse, json, logging
from cbm3data.accessdb import AccessDB
from simulation.nirsimulator import NIRSimulator
from simulation.nir_sql import nir_project_queries
from simulation.tools.avgdisturbanceextender import AvgDisturbanceExtender
from simulation.tools.disturbanceextender import DisturbanceExtender
from simulation.tools.disturbancegeneratorconfig import DisturbanceGeneratorConfig
from util import loghelper

def load_json(path):
    with open(path, 'r') as f:
        return json.loads(f.read())

def preprocess(config, project_path):
    with AccessDB(project_path) as nir_project_db:
        events_to_delete = config["EventsToDelete"]
        if len(events_to_delete)>0:
            sql_delete_events = nir_project_queries.sql_delete_disturbance_events(events_to_delete)
            nir_project_db.ExecuteQuery(sql_delete_events)

        post_delete_year = config["postDeleteYear"]
        sql_post_delete_year = nir_project_queries.sql_delete_post_year_events(post_delete_year)
        nir_project_db.ExecuteQuery(sql_post_delete_year)

        disturbance_extender_config = config["DisturbanceExtender"]
        if isinstance(disturbance_extender_config, list):
            avgDisturbanceExtender = AvgDisturbanceExtender()
            simpleDisturbanceExtender = DisturbanceExtender()
            logging.info("step 4 - run disturbance extender")
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
        else: logging.info("disturbance extender generator skipped")

        disturbance_generator_config = config["DisturbanceGenerator"]
        if isinstance(disturbance_generator_config, dict):
            logging.info("step 5 - run disturbance generator")
            disturbancegenerator = DisturbanceGeneratorConfig(
                os.path.abspath(disturbance_generator_config["ExePath"]),
                os.path.abspath(disturbance_generator_config["DefaultsPath"]),
                config["local_aidb_path"],
                disturbance_generator_config["Tasks"],
                projectTag, project.path)
            disturbancegenerator.Run()
        else: logging.info("disturbance generator skipped")

        nir_project_queries.sql_set_run_project_run_length(config["numTimeSteps"])
        nir_project_queries.run_simulation_id_cleanup(nir_project_db)
        nir_project_queries.update_random_seed(nir_project_db)


def main():
    try:
        parser = argparse.ArgumentParser(description="RunNation v2 script: processes and runs a batch of NIR simulations")
        parser.add_argument("--configuration", help="run nation configuration file")
        parser.add_argument("--prefix_filter", help="optional comma delimited prefixes, if included only the specified projects will be included")
        parser.add_argument("--copy_local", help="if present, copy the projects and archive index to the local working dir")
        parser.add_argument("--preprocess", help="if present, run the pre-processing steps on the local copies of project databases")
        parser.add_argument("--simulate", help="if present, run the simulations for each of the local copies of project databases")
        parser.add_argument("--load_python", help="if present, load the simulation results using the python loader")
        parser.add_argument("--rollup", help="if present, run the simulation rollup")

        args = parser.parse_args()

        config_path = os.path.abspath(args.configuration)
        config = load_json(args.configuration)
        working_dir = os.path.abspath(config["working_dir"])

        logpath = os.path.join(working_dir,
                "".join([datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S"),
                        config["logFileName"]]))
        loghelper.start_logging(logpath, 'w+')

        project_prefixes = config["project_prefixes"] \
            if not args.prefix_filter else \
            [x for x in config["project_prefixes"] 
             if config["project_prefixes"] in args.prefix_filter.split(",")]

        ns = NIRSimulator(config)

        if args.copy_local:
            ns.copy_aidb_local()
            for p in project_prefixes:
                ns.copy_project_local(p)

        if args.preprocess:
            for p in project_prefixes:
                local_project_path = ns.get_local_project_path(p)
                preprocess(config, local_project_path)

        if args.simulate:
            for p in project_prefixes:
                ns.run_cbm(p)

        if args.load_python:
            for p in project_prefixes:
                ns.load_project_results(p)

        if args.rollup:
            rrdbs = [ns.get_local_results_path(p) for p in project_prefixes]
            ns.do_rollup(rrdbs)

        #ns.run_cbm_simulation(ns.get_local_project_path(


    except Exception as ex:
        logging.exception("")


if __name__ == 'main':
    main()