import os, shutil
from simulation.simulator import Simulator
from cbm3data.aidb import AIDB
from cbm3data.accessdb import AccessDB
from cbm3data.projectdb import ProjectDB
from cbm3data.resultsloader import ResultsLoader

if __name__ == '__main__':
    import argparse

    toolbox_installation_dir = r"C:\Program Files (x86)\Operational-Scale CBM-CFS3"
    aidb_path = os.path.join(toolbox_installation_dir, "admin", "dbs", "ArchiveIndex_Beta_Install.mdb")
    cbm_exe_path = os.path.join(toolbox_installation_dir, "admin", "executables")
    working_aidb_path = os.path.join(toolbox_installation_dir, "admin", "dbs", "ArchiveIndex_Beta_Install_working.mdb")
    shutil.copyfile(aidb_path, working_aidb_path)
    project_path = r"C:\Program Files (x86)\Operational-Scale CBM-CFS3\Tutorials\Tutorial 3\Tutorial3.mdb"
    with AIDB(working_aidb_path, False) as aidb, \
         AccessDB(project_path, False) as proj:
        aidb.DeleteProjectsFromAIDB()
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
