import os
import unittest
import sqlite3

from cbm3_python.cbm3data import cbm3_results
from cbm3_python.cbm3data import cbm3_output_loader
from test.integration import import_run_helper


class CBMResultsIntegrationTests(unittest.TestCase):

    def test_integration(self):
        with import_run_helper.simulate() as sim:

            output_sqlite = os.path.join(sim.tempdir, "results_sqlite.db")
            cbm3_output_loader.load(
                loader_config={
                    "type": "db", "url": f"sqlite:///{output_sqlite}",
                    "chunksize": 10000},
                cbm_output_dir=os.path.join(
                    sim.tempfiles_dir, "CBMRun", "output"),
                project_db_path=sim.project_path,
                aidb_path=sim.aidb_path)

            cbm_results_db_flux_ind = cbm3_results.load_flux_indicators(
                sim.results_path, True, True, True, True, False)
            with sqlite3.connect(output_sqlite) as sqlite_con:
                cbm_results_db_flux_ind_sqlite = \
                    cbm3_results.load_flux_indicators(
                        sqlite_con, True, True, True, True, False)
            sqlite_con.close()
            cols_equal = \
                list(cbm_results_db_flux_ind.columns) == \
                list(cbm_results_db_flux_ind_sqlite.columns)
            self.assertTrue(cols_equal)
