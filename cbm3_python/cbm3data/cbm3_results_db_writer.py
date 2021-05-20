from sqlalchemy import Table
from sqlalchemy import MetaData
from cbm3_python.cbm3data import cbm3_results_db_schema
from sqlalchemy import create_engine


class CBMResultsDBWriter:

    def __init__(self, url, constraint_defs, create_engine_kwargs=None,
                 multi_update_variable_limit=None):
        """Create object to insert dataframes to a relational database.

        Args:
            url (str): url string containing db connection information passed
                to sqlalchemy.create_engine
            constraint_defs (dict): sqlalchemy constraint definitions. Nested
                dict of table_name.column_name.
            create_engine_kwargs (dict, optional): Extra keyword args passed
                to sqlalchemy.create_engine. Defaults to None.
            multi_update_variable_limit (int, optional): If specified the use
                the "multi" pandas.DataFrame.to_sql method for query batching.
                The integer value is used to set the upper limit on batch
                size. Defaults to None.
        """
        self.engine = \
            create_engine(url, **create_engine_kwargs) \
            if create_engine_kwargs else create_engine(url)
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

    def write(self, table_name, df):
        """Write the specified data using the sqlalchemy engine.

        If the specified table name has not already been created by this
        instance, it will be created. If the specified table name matches
        the name of an already existing table, and error will be raised.

        On any subsequent calls to this function with the same table_name, the
        specified data frame will be appended to the table.

        Args:
            table_name (str): the table name
            df (pandas.DataFrame): a pandas data frame to insert to the table.
        """
        if table_name not in self.created_tables:
            # create the table defintion
            table = Table(
                table_name, self.meta,
                *cbm3_results_db_schema.create_column_definitions(
                    table_name, df, self.constraint_defs))
            table.create(self.engine)
            self.created_tables[table_name] = table
        # insert the values in df
        table = self.created_tables[table_name]
        to_sql_kwargs = dict(
            name=table_name,
            con=self.engine,
            if_exists="append",
            index=False)
        if self.variable_limit:
            max_rows = self.variable_limit // len(df.columns)
            to_sql_kwargs.update(
                dict(method="multi", chunksize=max_rows))
        df.to_sql(**to_sql_kwargs)
