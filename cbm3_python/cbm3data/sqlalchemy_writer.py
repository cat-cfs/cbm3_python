import sqlalchemy
from sqlalchemy import Table, MetaData, Column


def map_pandas_dtype(dtype):
    if dtype == "int64":
        return sqlalchemy.Integer
    elif dtype == "float64":
        return sqlalchemy.Float
    elif dtype == "object":
        return sqlalchemy.String
    elif dtype == "bool":
        return sqlalchemy.Boolean
    else:
        raise ValueError(f"unmapped type {dtype}")


class SQLAlchemyWriter:

    def __init__(self, engine, constraint_defs):
        """

        Usage example::

            from sqlalchemy import create_engine

            if os.path.exists(os.path.abspath("test.db")):
                os.remove(os.path.abspath("test.db"))
            engine = create_engine("sqlite:///test.db")
            with SQLAlchemyWriter(engine, foreign_key_defs) as writer:
                cbm3_output_files_loader.load_output_relational_tables(
                    cbm_run_results_dir=cbm_run_results_dir,
                    project_db_path=project_db_path,
                    aidb_path=aidb_path,
                    out_func=writer.sqlalchemy_write,
                    chunksize=chunksize)

        Args:
            engine ([type]): [description]
            constraint_defs ([type]): [description]
        """

        self.engine = engine
        self.constraint_defs = constraint_defs
        self.variable_limit = 500
        self.meta = MetaData()
        self.created_tables = {}
        self.connection = None

    def __enter__(self):
        self.connection = self.engine.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.connection:
            self.connection.close()

    def _unpack_constraint_schema(self):
        pass

    def _create_column_definitions(self, name, df):
        result = []
        for i_column, column in enumerate(df.columns):

            column_args = [
                column,
                map_pandas_dtype(df.dtypes[i_column])]
            column_kwargs = {}

            column_constraint_args, column_constraint_kwargs = \
                self._unpack_constraint_schema(name, column)
            if column_constraint_args:
                column_args.extend(column_constraint_args)
            if column_constraint_kwargs:
                column_kwargs.update(column_constraint_kwargs)

            result.append(Column(*column_args, **column_kwargs))
        return result

    def sqlalchemy_write(self, name, df):
        if name not in self.created_tables:
            # create the table defintion
            table = Table(
                name, self.meta, *self._create_column_definitions(name, df))
            table.create(self.engine)
            self.created_tables[name] = table
        # insert the values in df
        table = self.created_tables[name]
        df.to_sql(name, self.engine, if_exists="append", index=False)
