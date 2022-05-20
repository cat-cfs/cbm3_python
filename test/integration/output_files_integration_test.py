import os
from types import SimpleNamespace
import unittest
import pandas as pd
import numpy as np

from cbm3_python import toolbox_defaults
from cbm3_python.cbm3data import cbm3_results
from cbm3_python.cbm3data import cbm3_output_files_loader
from test.integration import import_run_helper


class OutputFilesIntegrationTests(unittest.TestCase):
    def test_integration(self):

        with import_run_helper.simulate() as sim:
            cbm_results_db_flux_ind = cbm3_results.load_flux_indicators(
                sim.results_path
            )

            def create_out_func(output_table_name_pair):
                def out_func(name, df):
                    if name == output_table_name_pair.name:
                        output_table_name_pair.df = pd.concat(
                            [output_table_name_pair.df, df]
                        )

                return out_func

            descriptive_flux_indicators = SimpleNamespace(
                name="tblFluxIndicators", df=pd.DataFrame()
            )
            cbm3_output_files_loader.load_output_descriptive_tables(
                cbm_output_dir=os.path.join(
                    sim.tempfiles_dir, "CBMRun", "output"
                ),
                project_db_path=sim.project_path,
                aidb_path=toolbox_defaults.get_archive_index_path(),
                out_func=create_out_func(descriptive_flux_indicators),
                chunksize=10,
            )

            relational_flux_indicators = SimpleNamespace(
                name="tblFluxIndicators", df=pd.DataFrame()
            )
            cbm3_output_files_loader.load_output_relational_tables(
                cbm_output_dir=os.path.join(
                    sim.tempfiles_dir, "CBMRun", "output"
                ),
                project_db_path=sim.project_path,
                aidb_path=toolbox_defaults.get_archive_index_path(),
                out_func=create_out_func(relational_flux_indicators),
                chunksize=12,
            )

            a = (
                descriptive_flux_indicators.df[cbm_results_db_flux_ind.columns]
                .groupby("TimeStep")
                .sum()
                .reset_index()
                .sort_values(by="TimeStep")
            )

            b = (
                relational_flux_indicators.df[cbm_results_db_flux_ind.columns]
                .groupby("TimeStep")
                .sum()
                .reset_index()
                .sort_values(by="TimeStep")
            )

            self.assertTrue(
                np.allclose(
                    a.to_numpy(dtype=float),
                    cbm_results_db_flux_ind.to_numpy(dtype=float),
                )
            )

            self.assertTrue(
                np.allclose(
                    b.to_numpy(dtype=float),
                    cbm_results_db_flux_ind.to_numpy(dtype=float),
                )
            )
