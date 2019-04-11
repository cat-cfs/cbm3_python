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

        parser.add_argument("--project_path",
            help="path to a cbm project database file")
        parser.add_argument("--aidb_path", nargs="?",
            help="aidb_path path to a CBM-CFS3 archive index database")
        parser.add_argument("--toolbox_installation_dir", nargs="?",
            help="the Operational-Scale CBM-CFS3 toolbox installation "
                 "directory. If unspecified a typical default value is used.")
        parser.add_argument("--cbm_exe_path", nargs="?", 
            help="directory containing CBM.exe and Makelist.exe. If unspecified "
                 "a typical default value is used.")
        parser.add_argument("--results_database_path", nargs="?",
            help="optional file path into which CBM results will be loaded."
                 "if unspecified a default value is used.")
        parser.add_argument("--tempfiles_output_dir", nargs="?",
            help="optional directory where CBM tempfiles will be copied "
                 "after simulation.  If unspecified a default directory is "
                 "used.")
        parser.add_argument("--skip_makelist", action="store_true",
            help="If set then skip running makelist. Useful for "
                 "afforestation only projects and for re-using exising "
                 "makelist results.")
        parser.add_argument("--stdout_path", nargs="?",
            help="optionally redirect makelist and CBM standard output to "
                 "the specified file. If unspecified standard out is directed "
                 "to the console window")
        parser.add_argument("--use_existing_makelist_output", action="store_true",
            help="if set the existing values in the project "
                    "database will be used in place of a new set of values"
                    "generated by the makelist executable.")
        parser.add_argument("--dist_classes_path", nargs="?",
            help="one of a pair of optional file paths used to configure "
                 "extended kf6 accounting (requires NIR CBM.exe)")
        parser.add_argument("--dist_rules_path", nargs="?",
            help="one of a pair of optional file paths used to configure "
                 "extended kf6 accounting (requires NIR CBM.exe)")

        args = parser.parse_args()

        toolbox_installation_dir = r"C:\Program Files (x86)\Operational-Scale CBM-CFS3" \
            if not args.toolbox_installation_dir else os.path.abspath(args.toolbox_installation_dir)

        aidb_path = os.path.join(toolbox_installation_dir, "admin", "dbs", "ArchiveIndex_Beta_Install.mdb") \
            if not args.aidb_path else os.path.abspath(args.aidb_path)

        project_path = os.path.abspath(args.project_path)

        cbm_exe_path = os.path.join(toolbox_installation_dir, "admin", "executables") \
            if not args.cbm_exe_path else os.path.abspath(args.cbm_exe_path)

        results_database_path = None if not args.results_database_path else \
            os.path.abspath(args.results_database_path)

        tempfiles_output_dir = None if not args.tempfiles_output_dir else \
            os.path.abspath(args.tempfiles_output_dir)

        skip_makelist = True if args.skip_makelist else False

        stdout_path = None if not args.stdout_path else os.path.abspath(args.stdout_path)

        use_existing_makelist_output = True if args.use_existing_makelist_output else False

        dist_classes_path = None if not args.dist_classes_path else os.path.abspath(args.dist_classes_path)
        dist_rules_path = None if not args.dist_rules_path else os.path.abspath(args.dist_rules_path)

        results_path = projectsimulator.run(
            aidb_path, project_path, toolbox_installation_dir, cbm_exe_path,
            results_database_path=results_database_path,
            tempfiles_output_dir=tempfiles_output_dir,
            skip_makelist=skip_makelist,
            stdout_path=stdout_path,
            use_existing_makelist_output=use_existing_makelist_output,
            dist_classes_path=dist_classes_path,
            dist_rules_path=dist_rules_path)
        logging.info("simulation finish, results path: {0}"
                        .format(results_path))

    except Exception as ex:
        logging.exception("")


if __name__ == '__main__':
    main()