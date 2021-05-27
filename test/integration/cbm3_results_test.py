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

            flux_ind = cbm3_results.load_flux_indicators(
                sim.results_path, True, True, True, True, False)
            stock_changes = cbm3_results.load_stock_changes(
                sim.results_path, True, True, True, True, False)
            pool_ind = cbm3_results.load_pool_indicators(
                sim.results_path, True, True, True, False)
            age_ind = cbm3_results.load_age_indicators(
                sim.results_path, True, True, True, False)
            dist_ind = cbm3_results.load_disturbance_indicators(
                sim.results_path, True, True, True, False)
            with sqlite3.connect(output_sqlite) as sqlite_con:
                flux_ind_sqlite = \
                    cbm3_results.load_flux_indicators(
                        sqlite_con, True, True, True, True, False)
                stock_changes_sqlite = cbm3_results.load_stock_changes(
                    sim.results_path, True, True, True, True, False)
                pool_ind_sqlite = \
                    cbm3_results.load_pool_indicators(
                        sqlite_con, True, True, True, False)
                age_ind_sqlite = \
                    cbm3_results.load_age_indicators(
                        sqlite_con, True, True, True, False)
                dist_ind_sqlite = cbm3_results.load_disturbance_indicators(
                    sim.results_path, True, True, True, False)

            sqlite_con.close()

            self.assertTrue(
                list(flux_ind.columns) == list(flux_ind_sqlite.columns))
            self.assertTrue(
                list(stock_changes.columns) ==
                list(stock_changes_sqlite.columns))
            self.assertTrue(
                list(pool_ind.columns) == list(pool_ind_sqlite.columns))
            self.assertTrue(
                list(age_ind.columns) == list(age_ind_sqlite.columns))
            self.assertTrue(
                list(dist_ind.columns) == list(dist_ind_sqlite.columns))
