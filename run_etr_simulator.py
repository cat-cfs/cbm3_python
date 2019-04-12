# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import os, argparse, datetime
from cbm3_python.simulation.etrsimulator import ETRSimulator
from cbm3_python.util import loghelper
def get_date_stamp():
    return datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S")

def main():
    try:
        parser = argparse.ArgumentParser(description="ETR script: "
                                         "processes and runs a batch of NIR "
                                         "simulations with future projections")
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
        base_path_config_file = os.path.abspath(args.nir_base_path_config)
        local_working_dir = os.path.abspath(args.local_working_dir)
        es = ETRSimulator(config_path, base_path_config_file, local_working_dir)

        logpath = os.path.join(local_working_dir,
                 "{0}_{1}.log".format(date_stamp, config["Name"]))
        loghelper.start_logging(logpath, 'w+')

        es.run(args.prefix_filter,
               args.copy_local,
               args.preprocess,
               args.simulate,
               args.rollup,
               args.hwp_input,
               args.qaqc,
               args.copy_to_final_results_dir)

    except Exception as ex:
        logging.exception("")

if __name__ == '__main__':
    main()