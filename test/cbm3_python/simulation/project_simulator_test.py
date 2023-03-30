import os
import pytest
from tempfile import TemporaryDirectory
from unittest.mock import patch
from unittest.mock import MagicMock
from unittest.mock import call
from cbm3_python.simulation import projectsimulator
from cbm3_python.cbm3data import access_templates

PATCH_ROOT = "cbm3_python.simulation.projectsimulator"


@patch(f"{PATCH_ROOT}.setup_accounting_rules")
@patch(f"{PATCH_ROOT}.clear_old_results")
@patch(f"{PATCH_ROOT}.toolbox_defaults")
@patch(f"{PATCH_ROOT}.Simulator")
@patch(f"{PATCH_ROOT}.ProjectDB")
@patch(f"{PATCH_ROOT}.AIDB")
@patch(f"{PATCH_ROOT}.shutil")
@patch(f"{PATCH_ROOT}.os")
def test_case_1(
    os,
    shutil,
    aidb,
    project_db,
    simulator,
    toolbox_defaults,
    clear_old_results,
    setup_accounting_rules,
):
    toolbox_defaults.get_archive_index_path.side_effect = lambda: "aidb_path"
    toolbox_defaults.get_cbm_executable_dir.side_effect = (
        lambda: "cbm_exe_path"
    )
    toolbox_defaults.get_install_path.side_effect = (
        lambda: "toolbox_installation_dir"
    )
    mock_proj_instance = MagicMock()
    project_db.return_value.__enter__.return_value = mock_proj_instance
    mock_aidb_instance = MagicMock()
    aidb.return_value.__enter__.return_value = mock_aidb_instance
    mock_proj_instance.get_run_length.side_effect = (
        lambda x: "original_run_length"
    )
    mock_simulator = MagicMock()
    simulator.return_value = mock_simulator
    mock_aidb_instance.AddProjectToAIDB.return_value = "aidb_sim_id"

    os.path.dirname.return_value = "dirname_return_value"
    os.path.join.return_value = "join_return_value"
    mock_simulator.getDefaultArchiveIndexPath.return_value = (
        "aidb_path_original"
    )
    projectsimulator.run(
        project_path="project_path",
        project_simulation_id="project_simulation_id",
        n_timesteps="n_timesteps",
        aidb_path=None,
        toolbox_installation_dir=None,
        cbm_exe_path=None,
        results_database_path="results_database_path",
        tempfiles_output_dir="tempfiles_output_dir",
        skip_makelist=False,
        use_existing_makelist_output=False,
        copy_makelist_results=False,
        stdout_path="stdout_path",
        dist_classes_path="dist_classes_path",
        dist_rules_path="dist_rules_path",
        save_svl_by_timestep=True,
        loader_settings=None,
    )
    clear_old_results.assert_called_with(mock_proj_instance)
    toolbox_defaults.get_archive_index_path.assert_called()
    toolbox_defaults.get_cbm_executable_dir.assert_called()
    toolbox_defaults.get_install_path.assert_called()
    # confirm the tempfiles dir is removed
    shutil.rmtree.assert_called_with("tempfiles_output_dir")
    mock_aidb_instance.AddProjectToAIDB.assert_called_with(
        mock_proj_instance, project_sim_id="project_simulation_id"
    )
    mock_proj_instance.get_run_length.assert_called_with(
        "project_simulation_id"
    )
    mock_proj_instance.set_run_length.assert_has_calls(
        [
            call("n_timesteps", "project_simulation_id"),
            call("original_run_length", "project_simulation_id"),
        ]
    )
    simulator.assert_called_with(
        "cbm_exe_path",
        "aidb_sim_id",
        "dirname_return_value",
        "join_return_value",
        "toolbox_installation_dir",
        "stdout_path",
    )
    mock_simulator.setDefaultArchiveIndexPath.assert_has_calls(
        [call("aidb_path"), call("aidb_path_original")]
    )
    mock_simulator.removeCBMProjfile.assert_called_with("project_path")
    mock_simulator.CleanupRunDirectory.assert_called()

    mock_simulator.CreateMakelistFiles.assert_called()
    mock_simulator.copyMakelist.assert_called()
    mock_simulator.runMakelist.assert_called()
    mock_simulator.loadMakelistSVLS.assert_called()
    setup_accounting_rules.assert_called_with(
        "project_path", "dist_classes_path", "dist_rules_path"
    )
    mock_simulator.CopyToWorkingDir.assert_called_with("project_path")
    mock_simulator.DumpMakelistSVLs.assert_called()

    mock_simulator.CreateCBMFiles.assert_called_with(save_svl_by_timestep=True)
    mock_simulator.CopyCBMExecutable.assert_called()
    mock_simulator.RunCBM.assert_called()
    mock_simulator.CopyTempFiles.assert_called_with(
        output_dir="tempfiles_output_dir"
    )
    mock_simulator.LoadCBMResults.assert_called_with(
        output_path="results_database_path"
    )
    mock_aidb_instance.DeleteProjectsFromAIDB.assert_called_with("aidb_sim_id")


@patch(f"{PATCH_ROOT}.setup_accounting_rules")
@patch(f"{PATCH_ROOT}.clear_old_results")
@patch(f"{PATCH_ROOT}.toolbox_defaults")
@patch(f"{PATCH_ROOT}.CreateAccountingRules")
@patch(f"{PATCH_ROOT}.Simulator")
@patch(f"{PATCH_ROOT}.cbm3_output_loader")
@patch(f"{PATCH_ROOT}.ProjectDB")
@patch(f"{PATCH_ROOT}.AIDB")
@patch(f"{PATCH_ROOT}.shutil")
@patch(f"{PATCH_ROOT}.os")
def test_case_2(
    os,
    shutil,
    aidb,
    project_db,
    cbm3_output_loader,
    simulator,
    create_accounting_rules,
    toolbox_defaults,
    clear_old_results,
    setup_accounting_rules,
):
    toolbox_defaults.get_archive_index_path.side_effect = lambda: "aidb_path"
    toolbox_defaults.get_cbm_executable_dir.side_effect = (
        lambda: "cbm_exe_path"
    )
    toolbox_defaults.get_install_path.side_effect = (
        lambda: "toolbox_installation_dir"
    )
    mock_proj_instance = MagicMock()
    project_db.return_value.__enter__.return_value = mock_proj_instance
    mock_aidb_instance = MagicMock()
    aidb.return_value.__enter__.return_value = mock_aidb_instance
    mock_proj_instance.get_run_length.side_effect = (
        lambda x: "original_run_length"
    )
    mock_simulator = MagicMock()
    simulator.return_value = mock_simulator
    mock_aidb_instance.AddProjectToAIDB.return_value = "aidb_sim_id"

    os.path.dirname.return_value = "dirname_return_value"
    os.path.join.return_value = "join_return_value"
    mock_simulator.getDefaultArchiveIndexPath.return_value = (
        "aidb_path_original"
    )
    projectsimulator.run(
        project_path="project_path",
        project_simulation_id="project_simulation_id",
        n_timesteps="n_timesteps",
        aidb_path="my_aidb_path",
        toolbox_installation_dir="my_toolbox_installation_dir",
        cbm_exe_path="my_cbm_exe_path",
        results_database_path="results_database_path",
        tempfiles_output_dir="tempfiles_output_dir",
        skip_makelist=True,
        use_existing_makelist_output=False,
        copy_makelist_results="copy_makelist_results",
        stdout_path="stdout_path",
        dist_classes_path=None,
        dist_rules_path=None,
        save_svl_by_timestep=False,
        loader_settings={},
    )
    clear_old_results.assert_called_with(mock_proj_instance)

    # confirm the tempfiles dir is removed
    shutil.rmtree.assert_called_with("tempfiles_output_dir")
    mock_aidb_instance.AddProjectToAIDB.assert_called_with(
        mock_proj_instance, project_sim_id="project_simulation_id"
    )
    mock_proj_instance.get_run_length.assert_called_with(
        "project_simulation_id"
    )
    mock_proj_instance.set_run_length.assert_has_calls(
        [
            call("n_timesteps", "project_simulation_id"),
            call("original_run_length", "project_simulation_id"),
        ]
    )
    simulator.assert_called_with(
        "my_cbm_exe_path",
        "aidb_sim_id",
        "dirname_return_value",
        "join_return_value",
        "my_toolbox_installation_dir",
        "stdout_path",
    )
    mock_simulator.setDefaultArchiveIndexPath.assert_has_calls(
        [call("my_aidb_path"), call("aidb_path_original")]
    )
    mock_simulator.removeCBMProjfile.assert_called_with("project_path")
    mock_simulator.CleanupRunDirectory.assert_called()

    mock_simulator.CopyToWorkingDir.assert_called_with("project_path")
    mock_simulator.CreateEmptyMakelistOuput.assert_called()

    mock_simulator.loadMakelistSVLS.assert_called()

    mock_simulator.CopyToWorkingDir.assert_called_with("project_path")

    mock_simulator.copyMakelistOutput.assert_called_with(
        "copy_makelist_results"
    )
    mock_simulator.CreateCBMFiles.assert_called_with(
        save_svl_by_timestep=False
    )
    mock_simulator.CopyCBMExecutable.assert_called()
    mock_simulator.RunCBM.assert_called()
    mock_simulator.CopyTempFiles.assert_called_with(
        output_dir="tempfiles_output_dir"
    )
    mock_aidb_instance.DeleteProjectsFromAIDB.assert_called_with("aidb_sim_id")


@patch(f"{PATCH_ROOT}.clear_old_results")
@patch(f"{PATCH_ROOT}.toolbox_defaults")
@patch(f"{PATCH_ROOT}.Simulator")
@patch(f"{PATCH_ROOT}.cbm3_output_loader")
@patch(f"{PATCH_ROOT}.ProjectDB")
@patch(f"{PATCH_ROOT}.AIDB")
@patch(f"{PATCH_ROOT}.os")
def test_case_3(
    os,
    aidb,
    project_db,
    cbm3_output_loader,
    simulator,
    toolbox_defaults,
    clear_old_results,
):
    toolbox_defaults.get_archive_index_path.side_effect = lambda: "aidb_path"
    toolbox_defaults.get_cbm_executable_dir.side_effect = (
        lambda: "cbm_exe_path"
    )
    toolbox_defaults.get_install_path.side_effect = (
        lambda: "toolbox_installation_dir"
    )
    mock_proj_instance = MagicMock()
    project_db.return_value.__enter__.return_value = mock_proj_instance
    mock_aidb_instance = MagicMock()
    aidb.return_value.__enter__.return_value = mock_aidb_instance
    mock_proj_instance.get_run_length.side_effect = (
        lambda x: "original_run_length"
    )
    mock_simulator = MagicMock()
    simulator.return_value = mock_simulator
    mock_aidb_instance.AddProjectToAIDB.return_value = "aidb_sim_id"

    os.path.dirname.return_value = "dirname_return_value"
    os.path.join.return_value = "join_return_value"
    mock_simulator.getDefaultArchiveIndexPath.return_value = (
        "aidb_path_original"
    )
    projectsimulator.run(
        project_path="project_path",
        project_simulation_id="project_simulation_id",
        n_timesteps=None,
        aidb_path="my_aidb_path",
        toolbox_installation_dir="my_toolbox_installation_dir",
        cbm_exe_path="my_cbm_exe_path",
        results_database_path="results_database_path",
        tempfiles_output_dir=None,
        skip_makelist=True,
        use_existing_makelist_output=False,
        copy_makelist_results=True,
        stdout_path=None,
        dist_classes_path=None,
        dist_rules_path=None,
        save_svl_by_timestep=False,
        loader_settings={"type": 1},
    )
    clear_old_results.assert_called_with(mock_proj_instance)

    mock_aidb_instance.AddProjectToAIDB.assert_called_with(
        mock_proj_instance, project_sim_id="project_simulation_id"
    )

    simulator.assert_called_with(
        "my_cbm_exe_path",
        "aidb_sim_id",
        "dirname_return_value",
        "join_return_value",
        "my_toolbox_installation_dir",
        None,
    )
    mock_simulator.setDefaultArchiveIndexPath.assert_has_calls(
        [call("my_aidb_path"), call("aidb_path_original")]
    )
    mock_simulator.removeCBMProjfile.assert_called_with("project_path")
    mock_simulator.CleanupRunDirectory.assert_called()

    mock_simulator.CopyToWorkingDir.assert_called_with("project_path")
    mock_simulator.CreateEmptyMakelistOuput.assert_called()

    mock_simulator.loadMakelistSVLS.assert_called()

    mock_simulator.CopyToWorkingDir.assert_called_with("project_path")

    mock_simulator.copyMakelistOutput.assert_called()
    mock_simulator.CreateCBMFiles.assert_called_with(
        save_svl_by_timestep=False
    )
    mock_simulator.CopyCBMExecutable.assert_called()
    mock_simulator.RunCBM.assert_called()

    mock_aidb_instance.DeleteProjectsFromAIDB.assert_called_with("aidb_sim_id")

    cbm3_output_loader.load.assert_called_with(
        {"type": 1}, "join_return_value", "project_path", "my_aidb_path"
    )


def test_delete_old_tempfiles_error():
    with TemporaryDirectory() as tempdir:
        project_path = os.path.join(tempdir, "project.mdb")
        access_templates.copy_mdb_template(project_path)
        os.makedirs(os.path.join(tempdir, "tempfiles", "some_other_folder"))
        with pytest.raises(ValueError):
            projectsimulator.run(
                project_path,
                tempfiles_output_dir=os.path.join(tempdir, "tempfiles"),
            )


def test_non_abspath_error():
    with TemporaryDirectory() as tempdir:
        project_path = os.path.join(tempdir, "project.mdb")
        access_templates.copy_mdb_template(project_path)

        with pytest.raises(ValueError):
            projectsimulator.run(
                project_path,
                toolbox_installation_dir="toolbox_installation_dir",
            )


def test_non_existing_path_error():
    with TemporaryDirectory() as tempdir:
        project_path = os.path.join(tempdir, "project.mdb")
        access_templates.copy_mdb_template(project_path)

        with pytest.raises(ValueError):
            projectsimulator.run(
                project_path,
                toolbox_installation_dir=os.path.join(
                    tempdir, "toolbox_installation_dir"
                ),
            )


def test_conflicting_args_error():
    with TemporaryDirectory() as tempdir:
        project_path = os.path.join(tempdir, "project.mdb")
        access_templates.copy_mdb_template(project_path)

        with pytest.raises(ValueError):
            projectsimulator.run(
                project_path,
                use_existing_makelist_output=True,
                skip_makelist=False,
            )


@patch(f"{PATCH_ROOT}.AccessDB")
@patch(f"{PATCH_ROOT}.CreateAccountingRules")
def test_setup_accounting_rules(CreateAccountingRules, AccessDB):
    mock_accessdb_instance = MagicMock()
    AccessDB.return_value.__enter__.return_value = mock_accessdb_instance
    accounting_rules = MagicMock()
    CreateAccountingRules.return_value = accounting_rules
    projectsimulator.setup_accounting_rules(
        "project_path", "dist_classes_path", "dist_rules_path"
    )
    AccessDB.assert_called_with("project_path", False)
    CreateAccountingRules.assert_called_with(
        mock_accessdb_instance, "dist_classes_path", "dist_rules_path"
    )
    accounting_rules.create_accounting_rules.assert_called()
