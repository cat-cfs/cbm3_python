
import os
import unittest
from mock import patch
from cbm3_python import toolbox_defaults
from cbm3_python.scripts import simulate


class SimulateTest(unittest.TestCase):

    @patch("cbm3_python.scripts.simulate.projectsimulator")
    @patch("cbm3_python.scripts.simulate.loghelper")
    def test_keyword_args_defaults(self, loghelper, projectsimulator):
        simulate.main(["my_project.mdb"])
        projectsimulator.run.assert_called_with(
            project_path=os.path.abspath("my_project.mdb"),
            project_simulation_id=None,
            n_timesteps=None,
            aidb_path=toolbox_defaults.get_archive_index_path(),
            toolbox_installation_dir=toolbox_defaults.get_install_path(),
            cbm_exe_path=toolbox_defaults.get_cbm_executable_dir(),
            results_database_path=None,
            tempfiles_output_dir=None,
            skip_makelist=False,
            stdout_path=None,
            use_existing_makelist_output=False,
            copy_makelist_results=False,
            dist_classes_path=None,
            dist_rules_path=None,
            save_svl_by_timestep=False,
            loader_settings=None)

    @patch("cbm3_python.scripts.simulate.projectsimulator")
    @patch("cbm3_python.scripts.simulate.loghelper")
    def test_keyword_args(self, loghelper, projectsimulator):
        call_args = ["my_project.mdb"]
        call_kwargs = dict(
            project_simulation_id=2,
            n_timesteps=10,
            aidb_path="aidb.mdb",
            toolbox_installation_dir="toolbox_dir",
            cbm_exe_path="cbm_dir",
            results_database_path="results_path",
            tempfiles_output_dir="temp_dir",
            skip_makelist=True,
            stdout_path="stdout.txt",
            use_existing_makelist_output=True,
            copy_makelist_results=True,
            dist_classes_path="dist_classes",
            dist_rules_path="dist_rules",
            save_svl_by_timestep=True,
            loader_settings='{"a": "b"}')
        args = call_args.copy()
        for k, v in call_kwargs.items():
            args.append(f"--{k}")
            if v is not True:
                args.append(f"{v}")
        simulate.main(args)
        projectsimulator.run.assert_called_with(
            project_path=os.path.abspath("my_project.mdb"),
            project_simulation_id=2,
            n_timesteps=10,
            aidb_path=os.path.abspath(
                call_kwargs["aidb_path"]),
            toolbox_installation_dir=os.path.abspath(
                call_kwargs["toolbox_installation_dir"]),
            cbm_exe_path=os.path.abspath(
                call_kwargs["cbm_exe_path"]),
            results_database_path=os.path.abspath(
                call_kwargs["results_database_path"]),
            tempfiles_output_dir=os.path.abspath(
                call_kwargs["tempfiles_output_dir"]),
            skip_makelist=True,
            stdout_path=os.path.abspath(
                call_kwargs["stdout_path"]),
            use_existing_makelist_output=True,
            copy_makelist_results=True,
            dist_classes_path=os.path.abspath(
                call_kwargs["dist_classes_path"]),
            dist_rules_path=os.path.abspath(
                call_kwargs["dist_rules_path"]),
            save_svl_by_timestep=True,
            loader_settings={"a": "b"})
