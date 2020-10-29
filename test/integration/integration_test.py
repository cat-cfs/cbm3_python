import os
import tempfile
import unittest
from cbm3_python.cbm3data import sit_helper
import pandas as pd
from cbm3_python.cbm3data.accessdb import AccessDB


def get_db_table_names():
    return dict(
        age_class_table_name="sit_age_classes",
        classifiers_table_name="sit_classifiers",
        disturbance_events_table_name="sit_events",
        disturbance_types_table_name="sit_disturbance_types",
        inventory_table_name="sit_inventory",
        transition_rules_table_name="sit_transitions",
        yield_table_name="sit_yield")


def import_mdb_xls(working_dir, sit_mdb_path, mapping_file_path):
    os.makedirs(working_dir)
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

    import_args.update(get_db_table_names())

    sit_helper.mdb_xls_import(**import_args)
    return imported_project_path


def as_data_frame(query, access_db_path):
    with AccessDB(access_db_path) as access_db:
        df = pd.read_sql(query, access_db.connection)
    return df


def mdb_to_xls(sit_mdb_path, excel_output_path):
    for k, v in get_db_table_names().items():
        df = as_data_frame(f"SELECT * FROM {v}", sit_mdb_path)
        df.to_excel(excel_output_path, sheet_name=k, index=False)


def mdb_to_delimited(sit_mdb_path, ext, output_dir):
    if ext == ".tab":
        sep = "\t"
    elif ext == ".csv":
        sep = ","
    else:
        raise ValueError()
    for k, v in get_db_table_names().items():
        df = as_data_frame(f"SELECT * FROM {v}", sit_mdb_path)
        df.to_csv(os.path.join(output_dir, f"{k}{ext}"), sep=sep, index=False)


class IntegrationTests(unittest.TestCase):

    def test_integration(self):
        this_dir = os.path.dirname(os.path.realpath(__file__))
        mapping_file_path = os.path.join(this_dir, "mapping.json")
        sit_mdb_path = os.path.join(this_dir, "cbm3_sit.mdb")
        with tempfile.TemporaryDirectory() as tempdir:
            mdb_project = import_mdb_xls(
                os.path.join(tempdir, "mdb"), sit_mdb_path, mapping_file_path)
            xls_project = import_mdb_xls(
                os.path.join(tempdir, "xls"), mdb_to_xls(sit_mdb_path),
                mapping_file_path)
