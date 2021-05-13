from cbm3_python.cbm3data import cbm3_results_file_writer
from cbm3_python.cbm3data.cbm3_results_file_writer import CBM3ResultsFileWriter
from cbm3_python.cbm3data.cbm3_results_db_writer import CBMResultsDBWriter
from cbm3_python.cbm3data import cbm3_results_db_schema
from cbm3_python.cbm3data import cbm3_output_files_loader


def load(loader_config, cbm_output_dir, project_db_path, aidb_path):
    if loader_config["type"] in cbm3_results_file_writer.FORMATS:
        load_file(
            loader_config, cbm_output_dir, project_db_path, aidb_path)
    elif loader_config["type"] == "db":
        load_db(
            loader_config, cbm_output_dir, project_db_path, aidb_path)
    else:
        raise ValueError(
            f"unsupported loader_config type {loader_config['type']}")


def _parse_chunksize(loader_config):
    if "chunksize" in loader_config and loader_config["chunksize"] is not None:
        return loader_config["chunksize"]
    return None


def load_db(loader_config, cbm_output_dir, project_db_path, aidb_path):

    writer = CBMResultsDBWriter(
        loader_config["url"],
        cbm3_results_db_schema.get_constraints(),
        loader_config["create_engine_kwargs"]
        if "create_engine_kwargs" in loader_config else None,
        loader_config["multi_update_variable_limit"]
        if "multi_update_variable_limit" in loader_config else None)

    with writer:
        cbm3_output_files_loader.load_output_relational_tables(
            cbm_output_dir=cbm_output_dir,
            project_db_path=project_db_path,
            aidb_path=aidb_path,
            out_func=writer.write,
            chunksize=_parse_chunksize(loader_config))


def load_file(loader_config, cbm_output_dir, project_db_path, aidb_path):
    writer_kwargs = loader_config["writer_kwargs"] \
        if "writer_kwargs" in loader_config else None
    writer = CBM3ResultsFileWriter(
        loader_config["type"], loader_config["output_path"],
        writer_kwargs)
    cbm3_output_files_loader.load_output_descriptive_tables(
        cbm_output_dir=cbm_output_dir,
        project_db_path=project_db_path,
        aidb_path=aidb_path,
        out_func=writer.write,
        chunksize=_parse_chunksize(loader_config))
