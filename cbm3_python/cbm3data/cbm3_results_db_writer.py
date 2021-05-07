from sqlalchemy import Table
from sqlalchemy import MetaData
from cbm3_python.cbm3data import cbm3_results_db_schema
from sqlalchemy import create_engine


class CBMResultsDBWriter:

    def __init__(self, url, constraint_defs, create_engine_kwargs=None,
                 multi_update_variable_limit=None):
        """

        Usage example::

            from sqlalchemy import create_engine

            if os.path.exists(os.path.abspath("test.db")):
                os.remove(os.path.abspath("test.db"))
            engine = create_engine("sqlite:///test.db")
            with SQLAlchemyWriter(engine, foreign_key_defs) as writer:
                cbm3_output_files_loader.load_output_relational_tables(
                    cbm_output_dir=cbm_output_dir,
                    project_db_path=project_db_path,
                    aidb_path=aidb_path,
                    out_func=writer.sqlalchemy_write,
                    chunksize=chunksize)

        Args:
            engine ([type]): [description]
            constraint_defs ([type]): [description]
        """

        self.engine = create_engine(url)
        self.constraint_defs = constraint_defs
        self.variable_limit = multi_update_variable_limit
        self.meta = MetaData()
        self.created_tables = {}
        self.connection = None

    def __enter__(self):
        self.connection = self.engine.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.connection:
            self.connection.close()

    def write(self, name, df):
        if name not in self.created_tables:
            # create the table defintion
            table = Table(
                name, self.meta,
                *cbm3_results_db_schema.create_column_definitions(
                    name, df, self.constraint_defs))
            table.create(self.engine)
            self.created_tables[name] = table
        # insert the values in df
        table = self.created_tables[name]
        to_sql_kwargs = dict(
            name=name,
            con=self.engine,
            if_exists="append",
            index=False)
        if self.variable_limit:
            max_rows = self.variable_limit // len(df.columns)
            to_sql_kwargs.update(
                dict(method="multi", chunksize=max_rows))
        df.to_sql(**to_sql_kwargs)
