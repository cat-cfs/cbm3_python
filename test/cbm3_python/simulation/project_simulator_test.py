from unittest.mock import patch
from cbm3_python.simulation import projectsimulator

PATCH_ROOT = "cbm3_python.simulation.projectsimulator"


@patch(f"{PATCH_ROOT}.clear_old_results")
@patch(f"{PATCH_ROOT}.toolbox_defaults")
@patch(f"{PATCH_ROOT}.CreateAccountingRules")
@patch(f"{PATCH_ROOT}.Simulator")
@patch(f"{PATCH_ROOT}.cbm3_output_loader")
@patch(f"{PATCH_ROOT}.AccessDB")
@patch(f"{PATCH_ROOT}.ProjectDB")
@patch(f"{PATCH_ROOT}.AIDB")
@patch(f"{PATCH_ROOT}.shutil")
@patch(f"{PATCH_ROOT}.os")
def test_case(
    os,
    shutil,
    aidb,
    project_db,
    access_db,
    cbm3_output_loader,
    simulator,
    create_accounting_rules,
    toolbox_defaults,
    clear_old_results
):
    toolbox_defaults.get_archive_index_path.side_effect = lambda: "aidb_path"
    toolbox_defaults.get_cbm_executable_dir.side_effect = lambda: "cbm_exe_path"
    toolbox_defaults.get_install_path.side_effect = lambda: "toolbox_installation_dir"

    projectsimulator.run(
        project_path="project_path",
        project_simulation_id=1,
        n_timesteps=10,
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
        dist_rules_path="dist_classes_path",
        save_svl_by_timestep=True,
        loader_settings=None)
    clear_old_results.assert_called()
    toolbox_defaults.get_archive_index_path.assert_called()
    toolbox_defaults.get_cbm_executable_dir.assert_called()
    toolbox_defaults.get_install_path.assert_called()
