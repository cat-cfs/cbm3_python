from contextlib import contextmanager
from cbm3_python.cbm3data import cbm3_results_file_writer
from cbm3_python.cbm3data.cbm3_results_file_writer import CBM3ResultsFileWriter
from cbm3_python.cbm3data.cbm3_results_db_writer import CBMResultsDBWriter
from cbm3_python.cbm3data import cbm3_results_db_schema
from cbm3_python.cbm3data import cbm3_output_files_loader


@contextmanager
def get_db_writer(loader_config):
    """The loader_config parameter is a dictionary with the following fields:

      * url - a string passed to sqlalchemy.create_engine to create the
        database engine.
      * create_engine_kwargs - an optional dictionary specifying further
        keyword args to pass to sqlalchemy.create_engine function
      * multi_update_variable_limit - if specified enables the "multi" method
        of pandas.DataFrame.to_sql and also sets the upper bound for the number
        of variables per chunk.

    Args:
        loader_config (dict): a configuration dictionary

    Yields:
        object: an object to write dataframes, or dataframe chunks to a
            relational database.
    """

    writer = CBMResultsDBWriter(
        loader_config["url"],
        cbm3_results_db_schema.get_constraints(),
        loader_config["create_engine_kwargs"]
        if "create_engine_kwargs" in loader_config else None,
        loader_config["multi_update_variable_limit"]
        if "multi_update_variable_limit" in loader_config else None)
    with writer:
        yield writer


@contextmanager
def get_file_writer(loader_config):
    """Yield an object for writing loaded CBM results to file(s). The
    loader_config parameter is a dictionary with the following fields:

      * type - a string specifying "csv" or "hdf"
      * output_path - either a directory or filename depending on if
        the configured output is multiple files (csv) or a single file (hdf)
      * writer_kwargs - extra keyword arguments passed to pandas.to_csv if
        type "csv" or pandas.to_hdf if type is "hdf"

    Args:
        loader_config (dict): a configuration dictionary

    Yields:
        object: an object to write dataframes, or dataframe chunks to the
            configured file output.
    """
    writer_kwargs = loader_config["writer_kwargs"] \
        if "writer_kwargs" in loader_config else None
    writer = CBM3ResultsFileWriter(
        loader_config["type"], loader_config["output_path"],
        writer_kwargs)
    yield writer


def load(loader_config, cbm_output_dir, project_db_path, aidb_path):
    """Load CBM3 results into a database or descriptive file format using a
    built-in database or file writing method that is configured by the
    loader_config parameter.

    See :py:func:`get_file_writer` and :py:func:`get_db_writer` for
    documentation on configs.

    Args:
        loader_config (dict): a dictionary configuring the load process
        cbm_output_dir (str): path to the CBMRun/output dir
        project_db_path (str): path to the CBM3 project database
        aidb_path (str): path to the CBM3 archive index database

    Raises:
        ValueError: An unsupported loader type was specified
    """
    if loader_config["type"] in cbm3_results_file_writer.FORMATS:
        with get_file_writer(loader_config) as writer:
            load_file(writer, cbm_output_dir, project_db_path, aidb_path,
                      _parse_chunksize(loader_config))
    elif loader_config["type"] == "db":
        with get_db_writer(loader_config) as db_writer:
            load_db(db_writer, cbm_output_dir, project_db_path, aidb_path,
                    _parse_chunksize(loader_config))
    else:
        raise ValueError(
            f"unsupported loader_config type {loader_config['type']}")


def _parse_chunksize(loader_config):
    if "chunksize" in loader_config and loader_config["chunksize"] is not None:
        return loader_config["chunksize"]
    return None


def load_db(db_writer, cbm_output_dir, project_db_path, aidb_path,
            chunksize=None):
    """Load CBM3 results into a relational database.

    Args:
        db_writer (object): an object with a function
            write(table_name, pandas.DataFrame) accepting batches
            of data to write to a database.
        cbm_output_dir (str): path to the CBMRun/output dir
        project_db_path (str): path to the CBM3 project database
        aidb_path (str): path to the CBM3 archive index database
        chunksize (int, optional): If specified sets a maximum number of rows
            to hold in memory at a given time while loading output.
            Defaults to None.
    """

    cbm3_output_files_loader.load_output_relational_tables(
        cbm_output_dir=cbm_output_dir,
        project_db_path=project_db_path,
        aidb_path=aidb_path,
        out_func=db_writer.write,
        chunksize=chunksize)


def load_file(writer, cbm_output_dir, project_db_path, aidb_path,
              chunksize=None):
    """Loads CBM3 output using descriptive dataframes 

    Args:
        writer (object): an object with a function
            write(table_name, pandas.DataFrame) accepting batches
            of data to write to file(s).
        cbm_output_dir (str): path to the CBMRun/output dir
        project_db_path (str): path to the CBM3 project database
        aidb_path (str): path to the CBM3 archive index database
        chunksize (int, optional): If specified sets a maximum number of rows
            to hold in memory at a given time while loading output
            Defaults to None.
    """
    cbm3_output_files_loader.load_output_descriptive_tables(
        cbm_output_dir=cbm_output_dir,
        project_db_path=project_db_path,
        aidb_path=aidb_path,
        out_func=writer.write,
        chunksize=chunksize)
