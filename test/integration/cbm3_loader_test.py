import os
import pandas as pd
import numpy as np
import unittest
import sqlite3
from cbm3_python.cbm3data import cbm3_results
from cbm3_python.cbm3data import cbm3_output_loader
from test.integration import import_run_helper


class CBMOutputLoaderTest(unittest.TestCase):
    def test_save_svl_by_timestep_feature(self):
        with import_run_helper.simulate(save_svl_by_timestep=True) as sim:

            output_sqlite = os.path.join(sim.tempdir, "results_sqlite.db")
            cbm3_output_loader.load(
                loader_config={
                    "type": "db",
                    "url": f"sqlite:///{output_sqlite}",
                    "chunksize": None,
                },
                cbm_output_dir=os.path.join(
                    sim.tempfiles_dir, "CBMRun", "output"
                ),
                project_db_path=sim.project_path,
                aidb_path=sim.aidb_path,
            )

            with sqlite3.connect(output_sqlite) as sqlite_con:
                stock_changes_sqlite = cbm3_results.load_pool_indicators(
                    sqlite_con
                )
                tblSVL = pd.read_sql("SELECT * from tblSVL", sqlite_con)

                stock_changes_total_dom = (
                    stock_changes_sqlite[["TimeStep", "Dead Organic Matter"]]
                    .groupby("TimeStep")
                    .sum()
                )
                tbl_svl_total_dom = tblSVL[
                    ["TimeStep", "Area", "TotalDOMC_Density"]
                ]
                tbl_svl_total_dom["Dead Organic Matter"] = (
                    tbl_svl_total_dom.Area
                    * tbl_svl_total_dom.TotalDOMC_Density
                )
                tbl_svl_total_dom = tbl_svl_total_dom.groupby("TimeStep").sum()
                comparison = stock_changes_total_dom.merge(
                    tbl_svl_total_dom,
                    left_index=True,
                    right_index=True,
                    suffixes=("_a", "_b"),
                )
                self.assertTrue(
                    list(stock_changes_total_dom.index.unique())
                    == list(tbl_svl_total_dom.index.unique())
                )
                self.assertTrue(
                    np.allclose(
                        comparison["Dead Organic Matter_a"],
                        comparison["Dead Organic Matter_b"],
                    )
                )

            sqlite_con.close()

    def test_load_methods_sqlite(self):
        with import_run_helper.simulate() as sim:

            output_sqlite1 = os.path.join(sim.tempdir, "results_sqlite.db")
            cbm3_output_loader.load(
                loader_config={
                    "type": "db",
                    "url": f"sqlite:///{output_sqlite1}",
                    "chunksize": None,
                },
                cbm_output_dir=os.path.join(
                    sim.tempfiles_dir, "CBMRun", "output"
                ),
                project_db_path=sim.project_path,
                aidb_path=sim.aidb_path,
            )

            with sqlite3.connect(output_sqlite1) as sqlite_con1:
                stock_changes_sqlite1 = cbm3_results.load_stock_changes(
                    sqlite_con1, True, True, True, True, False
                )
            sqlite_con1.close()

            output_sqlite2 = os.path.join(
                sim.tempdir, "results_sqlite_chunked.db"
            )
            cbm3_output_loader.load(
                loader_config={
                    "type": "db",
                    "url": f"sqlite:///{output_sqlite2}",
                    "chunksize": 5,
                },
                cbm_output_dir=os.path.join(
                    sim.tempfiles_dir, "CBMRun", "output"
                ),
                project_db_path=sim.project_path,
                aidb_path=sim.aidb_path,
            )

            with sqlite3.connect(output_sqlite2) as sqlite_con2:
                stock_changes_sqlite2 = cbm3_results.load_stock_changes(
                    sqlite_con2, True, True, True, True, False
                )
            sqlite_con2.close()
            self.assertTrue(
                stock_changes_sqlite1.equals(stock_changes_sqlite2)
            )

    def test_load_methods_files(self):
        with import_run_helper.simulate() as sim:

            hdf1_path = os.path.join(sim.tempdir, "hdf1")
            hdf2_path = os.path.join(sim.tempdir, "hdf2")

            cbm3_output_loader.load(
                loader_config={
                    "type": "hdf",
                    "output_path": hdf1_path,
                    "chunksize": None,
                },
                cbm_output_dir=os.path.join(
                    sim.tempfiles_dir, "CBMRun", "output"
                ),
                project_db_path=sim.project_path,
                aidb_path=sim.aidb_path,
            )

            cbm3_output_loader.load(
                loader_config={
                    "type": "hdf",
                    "output_path": hdf2_path,
                    "chunksize": 64,
                },
                cbm_output_dir=os.path.join(
                    sim.tempfiles_dir, "CBMRun", "output"
                ),
                project_db_path=sim.project_path,
                aidb_path=sim.aidb_path,
            )

            # hdf1_result = pd.read_hdf(hdf1_path, "tblFluxIndicators")
            # hdf2_result = pd.read_hdf(hdf2_path, "tblFluxIndicators")
            # self.assertTrue(hdf1_result.equals(hdf2_result))

            csv1_path = os.path.join(sim.tempdir, "csv1")
            csv2_path = os.path.join(sim.tempdir, "csv2")

            cbm3_output_loader.load(
                loader_config={
                    "type": "csv",
                    "output_path": csv1_path,
                    "chunksize": None,
                },
                cbm_output_dir=os.path.join(
                    sim.tempfiles_dir, "CBMRun", "output"
                ),
                project_db_path=sim.project_path,
                aidb_path=sim.aidb_path,
            )

            cbm3_output_loader.load(
                loader_config={
                    "type": "csv",
                    "output_path": csv2_path,
                    "chunksize": 13,
                },
                cbm_output_dir=os.path.join(
                    sim.tempfiles_dir, "CBMRun", "output"
                ),
                project_db_path=sim.project_path,
                aidb_path=sim.aidb_path,
            )

            csv1_result = pd.read_csv(
                os.path.join(csv1_path, "tblFluxIndicators.csv")
            )
            csv2_result = pd.read_csv(
                os.path.join(csv2_path, "tblFluxIndicators.csv")
            )
            self.assertTrue(csv1_result.equals(csv2_result))
