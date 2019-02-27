import os
from cbm3_python.cbm3data.aidb import AIDB
from cbm3_python.cbm3data.accessdb import AccessDB
from cbm3_python.cbm3data.projectdb import ProjectDB
from cbm3_python.simulation.simulator import Simulator

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

