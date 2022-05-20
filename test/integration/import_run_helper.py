import os
import tempfile
from types import SimpleNamespace
from contextlib import contextmanager
from cbm3_python.simulation import projectsimulator
from cbm3_python import toolbox_defaults
from cbm3_python.cbm3data import sit_helper


def get_db_table_names():
    return dict(
        age_class_table_name="sit_age_classes",
        classifiers_table_name="sit_classifiers",
        disturbance_events_table_name="sit_events",
        disturbance_types_table_name="sit_disturbance_types",
        inventory_table_name="sit_inventory",
        transition_rules_table_name="sit_transitions",
        yield_table_name="sit_yield",
    )


def import_mdb(working_dir, sit_mdb_path, mapping_file_path):

    imported_project_path = os.path.join(working_dir, "cbm3_project.mdb")

    import_args = dict(
        mdb_xls_path=sit_mdb_path,
        imported_project_path=imported_project_path,
        mapping_path=mapping_file_path,
        initialize_mapping=False,
        archive_index_db_path=None,
        working_dir=working_dir,
        toolbox_install_dir=None,
    )

    table_names = get_db_table_names()

    import_args.update(table_names)

    sit_helper.mdb_xls_import(**import_args)
    return imported_project_path


@contextmanager
def simulate(**kwargs):
    this_dir = os.path.dirname(os.path.realpath(__file__))
    mapping_file_path = os.path.join(this_dir, "mapping.json")
    sit_mdb_path = os.path.join(this_dir, "cbm3_sit.mdb")
    with tempfile.TemporaryDirectory() as tempdir:
        project_path = import_mdb(tempdir, sit_mdb_path, mapping_file_path)
        results_path = os.path.join(tempdir, "results.mdb")
        tempfiles_dir = os.path.join(tempdir, "tempfiles")
        aidb_path = toolbox_defaults.get_archive_index_path()
        cbm_exe_path = toolbox_defaults.get_cbm_executable_dir()
        toolbox_path = toolbox_defaults.get_install_path()
        run_kwargs = dict(
            project_path=project_path,
            results_database_path=results_path,
            tempfiles_output_dir=tempfiles_dir,
            aidb_path=aidb_path,
            cbm_exe_path=cbm_exe_path,
        )
        run_kwargs.update(kwargs)
        list(
            projectsimulator.run_concurrent(
                run_args=[run_kwargs], toolbox_path=toolbox_path, max_workers=1
            )
        )

        yield SimpleNamespace(
            tempdir=tempdir,
            project_path=run_kwargs["project_path"],
            results_path=run_kwargs["results_database_path"],
            tempfiles_dir=run_kwargs["tempfiles_output_dir"],
            aidb_path=run_kwargs["aidb_path"],
            cbm_exe_path=run_kwargs["cbm_exe_path"],
            toolbox_path=toolbox_path,
        )
