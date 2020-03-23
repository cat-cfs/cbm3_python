# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import os, logging
from cbm3_python.util import loghelper
from cbm3_python.simulation.batchrunner import BatchRunner

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Multi-process CBM-CFS3 batch runner."
                    "Can run projects that contain multiple simulation scenarios partially in parallel. "
                    "Requires installation of the Operational-Scale CBM-CFS3 toolbox."
                    "This script has been used for deployment of the NIR Uncertainty analysis simulations")

    parser.add_argument("--projectPath", help = "path to a CBM-CFS3 project database", required=True)
    parser.add_argument("--outputPath", help = "directory where the simulation output will be written. This script will attempt to create it if it does not exist", required=True)
    parser.add_argument("--minID", help = "the inclusive minimum simulation ID to simulate. The simulation ids are defined in the project db table tblSimulation", required=True)
    parser.add_argument("--maxID", help = "the inclusive maximum simulation ID to simulate. The simulation ids are defined in the project db table tblSimulation", required=True)
    parser.add_argument("--dist_classes_path", help = "file path to the disturbance classes csv file used for extended accounting", required=True)
    parser.add_argument("--dist_rules_path", help = "file path to the disturbance classes csv file used for extended accounting", required=True)

    parser.add_argument("--aidb", help="the file path to the archive index database to use. If unspecified the default archive index is used", required=False)
    parser.add_argument("--exePath", help="the file path to a folder containing the CBM-CFS3 executables. If unspecified the default executables are used", required=False)

    args = vars(parser.parse_args())
    projectPath = args["projectPath"]
    outputPath =  args["outputPath"]
    minId = int(args["minID"])
    maxId = int(args["maxID"])
    dist_classes_path = args["dist_classes_path"]
    dist_rules_path = args["dist_rules_path"]
    aidb = args["aidb"] if "aidb" in args else r"C:\Program Files (x86)\Operational-Scale CBM-CFS3\Admin\DBs\ArchiveIndex_Beta_Install.mdb"
    exepath = args["exePath"] if "exePath" in args else r"C:\Program Files (x86)\Operational-Scale CBM-CFS3\Admin\Executables"

    if not os.path.exists(outputPath):
        os.makedirs(outputPath)

    logname = os.path.join(outputPath, "{0}.log".format(os.path.splitext(os.path.basename(projectPath))[0]))
    loghelper.start_logging(logname)
    try:
        b = BatchRunner(projectPath,
                        r"C:\Program Files (x86)\Operational-Scale CBM-CFS3",
                        r"C:\Program Files (x86)\Operational-Scale CBM-CFS3\batchruns",
                        aidb,
                        exepath,
                        outputPath,
                        multiprocessing.cpu_count(),
                        minId, maxId,dist_classes_path,
                        dist_rules_path)

        b.Run()
    except Exception as ex:
        logging.exception("error in batchrunner")
        raise ex
        
if __name__ == '__main__':
    main()