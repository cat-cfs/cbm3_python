import os
from cbm3_python.cbm3data.aidb import AIDB
from cbm3_python.cbm3data.accessdb import AccessDB
from cbm3_python.cbm3data.projectdb import ProjectDB
from cbm3_python.simulation.simulator import Simulator
from cbm3_python.simulation.tools.createaccountingrules import CreateAccountingRules

def clear_old_results(project_db):
    """
    Removes the data from project db tables tblSVLAttributes,
    tblSVLBySpeciesType.
    This is the equivalent of pushing the "reset" button in the
    simulation scheduler gui in the toolbox.  Each time CBM runs
    via the GUI, it loads both makelist and afforestation results
    into these tables, so for a re-run it's necessary to clear
    them out first.  The batching is here as a workaround to prevent
    msaccess/pyodbc error that can occur with large bulk updates/deletes.
    """

    ranges = project_db.get_batched_query_ranges(
        table_name="tblSVLAttributes",
        id_colname="SVOID",
        max_batch_size=50000)
    for range in ranges:
        project_db.ExecuteQuery(
            "delete from tblSVLAttributes where tblSVLAttributes.SVOID BETWEEN ? and ?",
            range)

def run(aidb_path, project_path, toolbox_installation_dir, cbm_exe_path,
       results_database_path=None, tempfiles_output_dir=None,
       skip_makelist=False, stdout_path=None, use_existing_makelist_output=False,
       dist_classes_path = None, dist_rules_path = None):
    '''
    runs the specified single simulation assumption project and loads the
    results
    @param aidb_path path to a CBM-CFS3 archive index database
    @param project_path path to a CBM-CFS3 project
    @param toolbox_installation_dir path of installed CBM-CFS3 Toolbox
    @param cbm_exe_path directory containing CBM.exe and Makelist.exe
    @param results_database_path if specified the path to a database that will
           be loaded with simulation results.  If unspecified a default path
           based on the simulation ID is used.  Either way the path is returned
           by this function.
    @param tempfiles_output_dir directory where CBM tempfiles, which are text
           based cbm and makelist input and output files will be copied. If
           unspecified these files will not be copied.
    @param skip_makelist when set to true the makelist routine will not be
           run. Can be used if the project contains no initially forested
           stands, or if existing makelist results are stored in the project
           database.
    @param stdout_path when set to a filepath will record all stdout from the
           different executables to this file instead of dumping them to the
           caller's stdout.
    @param use_existing_makelist_output if set to True, the existing values in
           the project database will be used in place of a new set of values
           generated by the makelist executable.
    @param dist_classes_path one of a pair of optional file paths used to 
           configure extended kf6 accounting (requires NIR CBM.exe)
    @param dist_rules_path one of a pair of optional file paths used to 
           configure extended kf6 accounting (requires NIR CBM.exe)
    '''
    with AIDB(aidb_path, False) as aidb, \
         AccessDB(project_path, False) as proj:


        if use_existing_makelist_output and not skip_makelist:
            raise ValueError("conflicting arguments: cannot both use "
                             "existing makelist output and run makelist")

        if not use_existing_makelist_output:
            clear_old_results(proj)

        simId = aidb.AddProjectToAIDB(proj)
        try:
            cbm_wd = os.path.join(toolbox_installation_dir, "temp")
            s = Simulator(cbm_exe_path,
                            simId,
                            os.path.dirname(project_path),
                            cbm_wd,
                            toolbox_installation_dir,
                            stdout_path)
            aidb_path_original = s.getDefaultArchiveIndexPath()
            s.setDefaultArchiveIndexPath(aidb_path)
            s.removeCBMProjfile(project_path)
            s.CleanupRunDirectory()

            if not skip_makelist:
                s.CreateMakelistFiles()
                s.copyMakelist()
                s.runMakelist()
            else:
                 # one side effect of "CreateMakelistFiles" is that the 
                 # project database is copied into the temp dir. When 
                 # skip_makelist is enabled this step is needed to copy
                 # the project into the temp dir
                 s.CopyToWorkingDir(project_path)
            if not use_existing_makelist_output:
                s.loadMakelistSVLS()
            if not dist_classes_path is None:
                #support for extended "kf6" results tracking
                with AccessDB(project_path, False) as proj:
                    cr = CreateAccountingRules(proj, dist_classes_path, dist_rules_path)
                    cr.create_accounting_rules()
                #copy the modified db to the working dir
                s.CopyToWorkingDir(project_path)

            s.DumpMakelistSVLs()
            s.CreateCBMFiles()
            s.CopyCBMExecutable()
            s.RunCBM()

            if tempfiles_output_dir:
                s.CopyTempFiles(output_dir=tempfiles_output_dir)
            s.LoadCBMResults(output_path = results_database_path)
        finally:
            s.setDefaultArchiveIndexPath(aidb_path_original)
            aidb.DeleteProjectsFromAIDB(simId) #cleanup
        results_path = s.getDefaultResultsPath() if results_database_path is None else results_database_path
        return results_path

