# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import os, shutil, argparse, datetime, logging
from cbm3_python.simulation.simulator import Simulator
from cbm3_python.cbm3data.aidb import AIDB
from cbm3_python.cbm3data.accessdb import AccessDB
from cbm3_python.cbm3data.projectdb import ProjectDB
from cbm3_python.util import loghelper
def run(aidb_path, project_path, toolbox_installation_dir, cbm_exe_path,
       results_database_path=None, tempfiles_output_dir=None):
    with AIDB(aidb_path, False) as aidb, \
         AccessDB(project_path, False) as proj:

        simId = aidb.AddProjectToAIDB(proj)
        try:
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
            s.CreateCBMFiles()
            s.CopyCBMExecutable()
            s.RunCBM()
            s.CopyTempFiles(output_dir=tempfiles_output_dir)
            s.LoadCBMResults(output_path = results_database_path)
        finally:
            aidb.DeleteProjectsFromAIDB(simId) #cleanup
        results_path = s.getDefaultResultsPath() if results_database_path is None else results_database_path
        return results_path

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
        args = parser.parse_args()

        toolbox_installation_dir = r"C:\Program Files (x86)\Operational-Scale CBM-CFS3" \
            if not args.toolbox_path else os.path.abspath(args.toolbox_path)
        results_db_path = None if not args.results_db_path else os.path.abspath(args.results_db_path)
        tempfiles_output_dir = None if not args.tempfiles_output_dir else os.path.abspath(args.tempfiles_output_dir)

        aidb_path = os.path.join(toolbox_installation_dir, "admin", "dbs", "ArchiveIndex_Beta_Install.mdb")
        cbm_exe_path = os.path.join(toolbox_installation_dir, "admin", "executables")
        project_path = os.path.abspath(args.projectdb)
        
        results_path = run(aidb_path, project_path, toolbox_installation_dir, cbm_exe_path,
                           results_database_path=results_db_path,
                           tempfiles_output_dir=tempfiles_output_dir)
        logging.info("simulation finish, results path: {0}"
                        .format(results_path))

    except Exception as ex:
        logging.exception("")


if __name__ == '__main__':
    main()