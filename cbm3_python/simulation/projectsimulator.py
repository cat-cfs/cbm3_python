import os
from cbm3_python.cbm3data.aidb import AIDB
from cbm3_python.cbm3data.accessdb import AccessDB
from cbm3_python.cbm3data.projectdb import ProjectDB
from cbm3_python.simulation.simulator import Simulator

def run(aidb_path, project_path, toolbox_installation_dir, cbm_exe_path,
       results_database_path=None, tempfiles_output_dir=None,
       afforestation_only=False):
    '''
    runs the specified single simulation assumption project and loads the results
    @param aidb_path path to a CBM-CFS3 archive index database
    @param project_path path to a CBM-CFS3 project
    @param toolbox_installation_dir path of installed CBM-CFS3 Toolbox
    @param cbm_exe_path directory containing CBM.exe and Makelist.exe 
    @param results_database_path if specified the path to a database that will
           be loaded with simulation results.  If unspecified a default path
           based on the simulation ID is used.  Either way the path is returned by this function.
    @param tempfiles_output_dir directory where CBM tempfiles, which are text
           based cbm and makelist input and output files will be copied. If unspecified these
           files will not be copied.
    @param afforestation_only when set to true the makelist routine will not be 
           run. Used if the project contains no initially forested stands. 
           For a mixture of non-forest, and forested stands, set to false (default)
    '''
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
            
            if not afforestation_only:
                s.CreateMakelistFiles()
                s.copyMakelist()
                s.runMakelist()
                s.loadMakelistSVLS()
                s.DumpMakelistSVLs()
                s.CreateCBMFiles()
                s.CopyCBMExecutable()
                s.RunCBM()
            else:
                s.CopyToWorkingDir(local_project_path)
                s.CreateCBMFiles()
                s.CopyCBMExecutable()
                s.DumpMakelistSVLs()
                s.RunCBM()
                s.CopyTempFiles()
            if tempfiles_output_dir:
                s.CopyTempFiles(output_dir=tempfiles_output_dir)
            s.LoadCBMResults(output_path = results_database_path)
        finally:
            aidb.DeleteProjectsFromAIDB(simId) #cleanup
        results_path = s.getDefaultResultsPath() if results_database_path is None else results_database_path
        return results_path

