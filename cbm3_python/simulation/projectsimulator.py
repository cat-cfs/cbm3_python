import os
import shutil
from cbm3_python.cbm3data.aidb import AIDB
from cbm3_python.cbm3data.projectdb import ProjectDB
from cbm3_python.cbm3data.accessdb import AccessDB
from cbm3_python.cbm3data.resultsloader import ResultsLoader
from cbm3_python.simulation.simulator import Simulator
from cbm3_python.simulation.tools.createaccountingrules \
    import CreateAccountingRules
from cbm3_python import toolbox_defaults


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
    for query_range in ranges:
        project_db.ExecuteQuery(
            "delete from tblSVLAttributes where tblSVLAttributes.SVOID "
            "BETWEEN ? and ?",
            query_range)


def _delete_old_tempfiles(tempfiles_output_dir):
    # do a little validation before dropping a rmtree on the wrong dir
    # due to a typo etc.
    ok_to_delete = \
        tempfiles_output_dir is not None and \
        os.path.exists(tempfiles_output_dir) and \
        os.path.isabs(tempfiles_output_dir) and \
        os.path.exists(os.path.join(tempfiles_output_dir, "CBMRun"))
    if ok_to_delete:
        shutil.rmtree(tempfiles_output_dir)


def run(project_path, project_simulation_id=None, n_timesteps=None,
        aidb_path=None, toolbox_installation_dir=None, cbm_exe_path=None,
        results_database_path=None, tempfiles_output_dir=None,
        skip_makelist=False, use_existing_makelist_output=False,
        copy_makelist_results=False, stdout_path=None, dist_classes_path=None,
        dist_rules_path=None, loader_settings=None):
    """runs the specified single simulation assumption project and loads the
    results

    Args:
        project_path (str): Path to a CBM-CFS3 project to simulate.
        project_simulation_id (int, optional): integer id for the simulation
            scenario to run in the project (tblSimulation.SimulationID). If
            not specified the highest numeric SimulationID is used.
        n_timesteps (int, optional): the number of timesteps to run the
            specified project. If not specified the value in tblRunTableDetails
            will be used.
        aidb_path (str, optional): Path to a CBM-CFS3 archive index database.
            If unspecified a typical default value is used. Defaults to None.
        toolbox_installation_dir (str, optional): Path of the installed
            CBM-CFS3 Toolbox. If unspecified a typical default value is used.
            Defaults to None.
        cbm_exe_path (str, optional): Directory containing CBM.exe and
            Makelist.exe. If unspecified a typical default value is used.
            Defaults to None.
        results_database_path (str, optional): The path to the database
            produced with simulation results. If unspecified a default path
            based on the simulation ID is used. The path is returned
            by this function. Defaults to None.
        tempfiles_output_dir (str, optional): Directory where CBM tempfiles,
            which are text based cbm and makelist input and output files, will
            be copied. If unspecified these files will not be copied. Defaults
            to None.
        skip_makelist (bool, optional): If set to true the makelist routine
            will not be run. Can be used if the project contains no initially
            forested stands, or if existing makelist results are stored in the
            project database. Defaults to False.
        use_existing_makelist_output (bool, optional): If set to True, the
            existing values in the project database will be used in place of a
            new set of values generated by the makelist executable. Defaults to
            False.
        copy_makelist_results (bool, optional): if set to True, makelist *svl
            output is directly copied from the makelist output dir to the cbm
            input dir rather than using the toolbox load svl and dump svl
            procedures. Defaults to False.
        stdout_path (str, optional): When set to a filepath all stdout from
            the CBM/Makelist executables will be redirected into the specified
            file. Defaults to None.
        dist_classes_path (str, optional): File 1 of 2 of a pair of optional
            file paths used to configure extended kf accounting (requires NIR
            CBM.exe). Defaults to None.
        dist_rules_path (str, optional): File 2 of 2 of a pair of optional
            file paths used to configure extended kf accounting (requires NIR
            CBM.exe). Defaults to None.
        loader_settings (dict, optional): If None the toolbox loader is used,
            otherwise this arg specifies loader specific settings. Defaults to
            None.

    Returns:
        str: The path to the loaded CBM results database
    """

    if not aidb_path:
        aidb_path = toolbox_defaults.ARCHIVE_INDEX_PATH
    if not cbm_exe_path:
        cbm_exe_path = toolbox_defaults.CBM_EXECUTABLE_DIR
    if not toolbox_installation_dir:
        toolbox_installation_dir = toolbox_defaults.INSTALL_PATH

    # don't allow relative paths here, it will cause failures later in CBM
    # command line apps that have difficult to understand error messages
    existing_paths = [
        project_path, aidb_path, cbm_exe_path, toolbox_installation_dir,
        dist_classes_path, dist_rules_path]
    output_paths = [results_database_path, tempfiles_output_dir, stdout_path]
    all_paths = existing_paths + output_paths
    for path in all_paths:
        if path is None:
            continue
        if not os.path.isabs(path):
            raise ValueError(
                "Relative paths detected. They may cause failures in CBM "
                f"model command line processes: '{path}'")
    for path in existing_paths:
        if path is None:
            continue
        if not os.path.exists(path):
            raise ValueError(f"specified path does not exist '{path}'")
    _delete_old_tempfiles(tempfiles_output_dir)

    with AIDB(aidb_path, False) as aidb, \
            ProjectDB(project_path, False) as proj:

        if use_existing_makelist_output and not skip_makelist:
            raise ValueError(
                "conflicting arguments: Cannot both use "
                "existing makelist output and run makelist.")

        if skip_makelist and copy_makelist_results:
            raise ValueError(
                "conflicting arguments: Cannot skip makelist "
                "and copy makelist output.")

        if not use_existing_makelist_output:
            clear_old_results(proj)

        simId = aidb.AddProjectToAIDB(
            proj, project_sim_id=project_simulation_id)
        original_run_length = None
        if n_timesteps:
            original_run_length = proj.get_run_length(project_simulation_id)
            proj.set_run_length(n_timesteps, project_simulation_id)
        try:
            cbm_wd = os.path.join(toolbox_installation_dir, "temp")
            s = Simulator(
                cbm_exe_path,
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
                s.CreateEmptyMakelistOuput()
            if not use_existing_makelist_output:
                s.loadMakelistSVLS()
            if dist_classes_path is not None:
                # support for extended "kf6" results tracking
                with AccessDB(project_path, False) as proj:
                    cr = CreateAccountingRules(proj, dist_classes_path,
                                               dist_rules_path)
                    cr.create_accounting_rules()
                # copy the modified db to the working dir
                s.CopyToWorkingDir(project_path)

            if not copy_makelist_results:
                s.DumpMakelistSVLs()
            else:
                s.copyMakelistOutput()
            s.CreateCBMFiles()
            s.CopyCBMExecutable()
            s.RunCBM()

            if tempfiles_output_dir:
                s.CopyTempFiles(output_dir=tempfiles_output_dir)
            if loader_settings is None:
                s.LoadCBMResults(output_path=results_database_path)
            elif loader_settings["type"] == "python_loader":
                r = ResultsLoader()
                r.loadResults(
                    outputDBPath=results_database_path,
                    aidbPath=aidb_path,
                    projectDBPath=project_path,
                    projectSimulationDirectory=cbm_wd)
            else:
                raise ValueError("unknown loader settings")
        finally:
            # cleanup
            if original_run_length:
                proj.set_run_length(original_run_length, project_simulation_id)
            s.setDefaultArchiveIndexPath(aidb_path_original)
            aidb.DeleteProjectsFromAIDB(simId)
        results_path = s.getDefaultResultsPath() if results_database_path \
            is None else results_database_path
        return results_path


def run_concurrent(run_args, toolbox_path):
    from cbm3_python.simulation.concurrent_runner import ConcurrentRunner
    runner = ConcurrentRunner(toolbox_path)
    for finished_task in runner.run(run_args):
        yield finished_task
