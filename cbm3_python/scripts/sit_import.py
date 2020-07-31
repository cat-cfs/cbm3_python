import argparse
import logging
from cbm3_python.cbm3data import sit_helper
from cbm3_python import toolbox_defaults


def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "sit_data_dir",
            help="directory containing SIT formatted csv files and parameter "
                 "mapping json file")
        parser.add_argument(
            "cbm3_project_path",
            help="path to the file created by imported the specified SIT "
                 "dataset")
        parser.add_argument(
            "--aidb_path",
            help="path to the archive index from which default CBM parameters "
                 "are drawn during the SIT import process.")
        parser.add_argument(
            "--initialize_mapping",
            help="if specified indicates the the values in the SIT dataset "
                 "are identical to values stored in the archive index and do "
                 "not require mapping.",
            action="store_true",
            dest="initialize_mapping")
        parser.add_argument(
            "--working_dir",
            help="Optional working dir where logs, and intermediate files are "
                 "created during the sit process.  If not specified a sub "
                 "directory in the specified 'sit_data_dir' is used.")

        args = parser.parse_args()

        aidb_path = args.aidb_path
        if not aidb_path:
            aidb_path = toolbox_defaults.ARCHIVE_INDEX_PATH

        sit_helper.csv_import(
            csv_dir=args.sit_data_dir,
            imported_project_path=args.cbm3_project_path,
            initialize_mapping=args.initialize_mapping,
            archive_index_db_path=aidb_path,
            working_dir=args.working_dir)
    except Exception:
        logging.exception("")


if __name__ == '__main__':
    main()
