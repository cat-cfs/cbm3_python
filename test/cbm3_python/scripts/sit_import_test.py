import unittest
from mock import patch
from cbm3_python import toolbox_defaults
from cbm3_python.scripts import sit_import


class SITImportTest(unittest.TestCase):

    @patch("cbm3_python.scripts.sit_import.sit_helper")
    def test_keyword_args_defaults(self, sit_helper):
        sit_import.main(["sit_data_dir", "cbm3_project_path"])
        sit_helper.csv_import.assert_called_with(
            csv_dir="sit_data_dir",
            imported_project_path="cbm3_project_path",
            initialize_mapping=False,
            archive_index_db_path=toolbox_defaults.get_archive_index_path(),
            working_dir=None)

    @patch("cbm3_python.scripts.sit_import.sit_helper")
    def test_keyword_args(self, sit_helper):
        sit_import.main([
            "sit_data_dir",
            "cbm3_project_path",
            "--initialize_mapping",
            "--aidb_path", "aidb_path",
            "--working_dir", "working_dir"])
        sit_helper.csv_import.assert_called_with(
            csv_dir="sit_data_dir",
            imported_project_path="cbm3_project_path",
            initialize_mapping=True,
            archive_index_db_path="aidb_path",
            working_dir="working_dir")
