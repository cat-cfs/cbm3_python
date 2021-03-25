import os
import tempfile
from types import SimpleNamespace
import unittest
import pandas as pd
import numpy as np
from cbm3_python import toolbox_defaults
from cbm3_python.cbm3data import sit_helper
from cbm3_python.simulation import projectsimulator
from cbm3_python.cbm3data import cbm3_results
from cbm3_python.cbm3data import cbm3_output_files_loader


def get_db_table_names():
    return dict(
        age_class_table_name="sit_age_classes",
        classifiers_table_name="sit_classifiers",
        disturbance_events_table_name="sit_events",
        disturbance_types_table_name="sit_disturbance_types",
        inventory_table_name="sit_inventory",
        transition_rules_table_name="sit_transitions",
        yield_table_name="sit_yield")


def import_mdb(working_dir, sit_mdb_path, mapping_file_path):

    imported_project_path = os.path.join(
        working_dir, "cbm3_project.mdb")

    import_args = dict(
        mdb_xls_path=sit_mdb_path,
        imported_project_path=imported_project_path,
        mapping_path=mapping_file_path,
        initialize_mapping=False,
        archive_index_db_path=None,
        working_dir=working_dir,
        toolbox_install_dir=None)

    table_names = get_db_table_names()

    import_args.update(table_names)

    sit_helper.mdb_xls_import(**import_args)
    return imported_project_path


class OutputFilesIntegrationTests(unittest.TestCase):

    def test_integration(self):
        this_dir = os.path.dirname(os.path.realpath(__file__))
        mapping_file_path = os.path.join(this_dir, "mapping.json")
        sit_mdb_path = os.path.join(this_dir, "cbm3_sit.mdb")
        with tempfile.TemporaryDirectory() as tempdir:
            project_path = import_mdb(
                tempdir, sit_mdb_path, mapping_file_path)
            results_path = os.path.join(tempdir, "results.mdb")
            tempfiles_dir = os.path.join(tempdir, "tempfiles")
            list(projectsimulator.run_concurrent(
                run_args=[dict(
                    project_path=project_path,
                    results_database_path=results_path,
                    tempfiles_output_dir=tempfiles_dir,
                    aidb_path=toolbox_defaults.get_archive_index_path(),
                    cbm_exe_path=toolbox_defaults.get_cbm_executable_dir()
                )],
                toolbox_path=toolbox_defaults.get_install_path(),
                max_workers=1))

            cbm_results_db_flux_ind = cbm3_results.load_flux_indicators(
                results_path)

            def create_out_func(output_table_name_pair):
                def out_func(name, df):
                    if name == output_table_name_pair.name:
                        output_table_name_pair.df = \
                            output_table_name_pair.df.append(df)
                return out_func

            descriptive_flux_indicators = SimpleNamespace(
                name="flux_indicators",
                df=pd.DataFrame())
            cbm3_output_files_loader.load_output_descriptive_tables(
                cbm_run_results_dir=os.path.join(
                    tempfiles_dir, "CBMRun", "output"),
                project_db_path=project_path,
                aidb_path=toolbox_defaults.get_archive_index_path(),
                out_func=create_out_func(descriptive_flux_indicators),
                chunksize=10)

            relational_flux_indicators = SimpleNamespace(
                name="tblFluxIndicators",
                df=pd.DataFrame())
            cbm3_output_files_loader.load_output_relational_tables(
                cbm_run_results_dir=os.path.join(
                    tempfiles_dir, "CBMRun", "output"),
                project_db_path=project_path,
                aidb_path=toolbox_defaults.get_archive_index_path(),
                out_func=create_out_func(relational_flux_indicators),
                chunksize=12)

            self.assertTrue(
                np.allclose(
                    descriptive_flux_indicators.df[
                        cbm_results_db_flux_ind.columns
                    ].groupby("TimeStep").sum().reset_index()
                     .sort_values(by="TimeStep").to_numpy(),
                    cbm_results_db_flux_ind.to_numpy()))

            self.assertTrue(
                np.allclose(
                    relational_flux_indicators.df[
                        cbm_results_db_flux_ind.columns
                    ].groupby("TimeStep").sum().reset_index()
                     .sort_values(by="TimeStep").to_numpy(),
                    cbm_results_db_flux_ind.to_numpy()))
