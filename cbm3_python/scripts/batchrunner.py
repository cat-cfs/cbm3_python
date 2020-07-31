# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import os
import logging
import argparse
import multiprocessing
from cbm3_python.util import loghelper
from cbm3_python.simulation.batchrunner import BatchRunner
from cbm3_python import toolbox_defaults


def main():

    parser = argparse.ArgumentParser(
        description="Multi-process CBM-CFS3 batch runner. Can run projects "
                    "that contain multiple simulation scenarios partially in "
                    "parallel. This requires installation of the "
                    "Operational-Scale CBM-CFS3 toolbox. This script has been "
                    "used for deployment of the NIR Uncertainty analysis "
                    "simulations")

    parser.add_argument(
        "--projectPath", required=True,
        help="path to a CBM-CFS3 project database")
    parser.add_argument(
        "--outputPath", required=True,
        help="Directory where the simulation output will be written. This "
             "script will attempt to create it if it does not exist", )
    parser.add_argument(
        "--minID", required=True,
        help="The inclusive minimum simulation ID to simulate. The simulation "
             "ids are defined in the project db table tblSimulation")
    parser.add_argument(
        "--maxID", required=True,
        help="The inclusive maximum simulation ID to simulate. The simulation "
             "ids are defined in the project db table tblSimulation")
    parser.add_argument(
        "--dist_classes_path", required=True,
        help="file path to the disturbance classes csv file used for extended "
             "accounting")
    parser.add_argument(
        "--dist_rules_path", required=True,
        help="file path to the disturbance classes csv file used for extended "
             "accounting")
    parser.add_argument(
        "--aidb", required=False,
        help="the file path to the archive index database to use. If "
             "unspecified the default archive index is used")
    parser.add_argument(
        "--exePath", required=False,
        help="the file path to a folder containing the CBM-CFS3 executables."
             "If unspecified the default executables are used")

    args = vars(parser.parse_args())
    projectPath = args["projectPath"]
    outputPath = args["outputPath"]
    minId = int(args["minID"])
    maxId = int(args["maxID"])
    dist_classes_path = args["dist_classes_path"]
    dist_rules_path = args["dist_rules_path"]
    aidb = \
        args["aidb"] if "aidb" in args else toolbox_defaults.ARCHIVE_INDEX_PATH
    exe_path = \
        args["exePath"] if "exePath" in args \
        else toolbox_defaults.CBM_EXECUTABLE_DIR

    if not os.path.exists(outputPath):
        os.makedirs(outputPath)

    logname = os.path.join(
        outputPath,
        "{0}.log".format(os.path.splitext(os.path.basename(projectPath))[0]))
    loghelper.start_logging(logname)
    try:
        b = BatchRunner(
            projectPath,
            toolbox_defaults.INSTALL_PATH,
            os.path.join(toolbox_defaults.INSTALL_PATH, "batchruns"),
            aidb,
            exe_path,
            outputPath,
            multiprocessing.cpu_count(),
            minId, maxId, dist_classes_path,
            dist_rules_path)

        b.Run()
    except Exception as ex:
        logging.exception("error in batchrunner")
        raise ex


if __name__ == '__main__':
    main()
