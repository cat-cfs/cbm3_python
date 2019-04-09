# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import os, shutil, argparse, datetime, logging
import cbm3_python.simulation.projectsimulator as projectsimulator

from cbm3_python.util import loghelper


def main():
    try:
        logpath = os.path.join(
                    "{0}_{1}.log".format(
                        "simulation",
                        datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S")))
        loghelper.start_logging(logpath, 'w+')

        parser = argparse.ArgumentParser(description="CBM-CFS3 simulation "
            "script. Simulates a CBM-CFS3 project access database by "
            "automating functions in the Operational-Scale CBM-CFS3 toolbox")
        parser.add_argument("--projectdb", help="path to a cbm project database file")
        parser.add_argument("--toolbox_path", nargs="?",
                            help="the Operational-Scale CBM-CFS3 toolbox "
                            "installation directory. If unspecified a the "
                            "typical default value is used.")
        parser.add_argument("--tempfiles_output_dir", nargs="?",
                    help="optional directory where CBM tempfiles will be copied "
                    "after simulation.  If unspecified a default directory is used.")
        parser.add_argument("--results_db_path", nargs="?",
                    help="optional file path into which CBM results will be loaded."
                    "if unspecified a default value is used.")
        parser.add_argument("--afforestation_only", nargs="?",
                    help="if the specified projectdb contains only initially "
                    "non-forested stands, set this option to true")
        parser.add_argument("--stdout_path", nargs="?",
                    help="optionally redirect makelist and CBM standard "
                    "output to the specified file. If unspecified standard "
                    "out is directed to the console window")
        args = parser.parse_args()

        toolbox_installation_dir = r"C:\Program Files (x86)\Operational-Scale CBM-CFS3" \
            if not args.toolbox_path else os.path.abspath(args.toolbox_path)
        results_db_path = None if not args.results_db_path else os.path.abspath(args.results_db_path)
        tempfiles_output_dir = None if not args.tempfiles_output_dir else os.path.abspath(args.tempfiles_output_dir)
        stdout_path = None if not args.stdout_path else os.path.abspath(args.stdout_path)

        aidb_path = os.path.join(toolbox_installation_dir, "admin", "dbs", "ArchiveIndex_Beta_Install.mdb")
        cbm_exe_path = os.path.join(toolbox_installation_dir, "admin", "executables")
        project_path = os.path.abspath(args.projectdb)
        afforestation_only = True if args.afforestation_only else False
        results_path = projectsimulator.run(
            aidb_path, project_path, toolbox_installation_dir, cbm_exe_path,
                           results_database_path=results_db_path,
                           tempfiles_output_dir=tempfiles_output_dir,
                           afforestation_only=afforestation_only,
                           stdout_path=stdout_path)
        logging.info("simulation finish, results path: {0}"
                        .format(results_path))

    except Exception as ex:
        logging.exception("")


if __name__ == '__main__':
    main()