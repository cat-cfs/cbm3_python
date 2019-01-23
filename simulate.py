# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import os, shutil, argparse, datetime, logging
from simulation.simulator import Simulator
from cbm3data.aidb import AIDB
from cbm3data.accessdb import AccessDB
from cbm3data.projectdb import ProjectDB

from util import loghelper
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
        args = parser.parse_args()

        toolbox_installation_dir = r"C:\Program Files (x86)\Operational-Scale CBM-CFS3" \
            if not args.toolbox_path else os.path.abspath(args.toolbox_path)

        aidb_path = os.path.join(toolbox_installation_dir, "admin", "dbs", "ArchiveIndex_Beta_Install.mdb")
        cbm_exe_path = os.path.join(toolbox_installation_dir, "admin", "executables")
        project_path = os.path.abspath(args.projectdb)

        with AIDB(aidb_path, False) as aidb, \
             AccessDB(project_path, False) as proj:

            simId = aidb.AddProjectToAIDB(proj)
            cbm_wd = os.path.join(toolbox_installation_dir, "temp")
            s = Simulator(cbm_exe_path,
                            simId,
                            os.path.dirname(project_path),
                            cbm_wd,
                            toolbox_installation_dir)

            s.CleanupRunDirectory()
            s.CreateMakelistFiles()
            s.copyMakelist()
            s.runMakelist()
            s.loadMakelistSVLS()
            s.DumpMakelistSVLs()
            s.copyMakelistOutput()
            s.CreateCBMFiles()
            s.CopyCBMExecutable()
            s.RunCBM()
            s.CopyTempFiles()
            s.LoadCBMResults()
            results_path = s.getResultsPath()
            logging.info("simulation finish, results path: {0}"
                         .format(results_path))

            
    except Exception as ex:
        logging.exception("")


if __name__ == '__main__':
    main()