# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import pyodbc
import logging
import os
from pyodbc import ProgrammingError
try:
    from collections.abc import Iterable
except ImportError:
    from collections import Iterable
import math
# Scott - Nov 2013
# wrapper for ms access object allowing queries
# and some other basic operations
class AccessDB(object):

    def __init__(self, path, log_enabled=True):
        self.path = path
        self.log_enabled = log_enabled
        self.connection_string = self.getConnectionString(path)

    def __enter__(self):
        self.connection = pyodbc.connect(self.connection_string, autocommit=False)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        if self.connection:
            self.connection.close()

    def getConnectionString(self, path):
        return "Driver={Microsoft Access Driver (*.mdb, *.accdb)};User Id='admin';Dbq=" + path    

    def _floatifyIntParams(self, params):
        """
        workaround for access/pyodbc bug. Converts integer sql parameters to float 
        see https://stackoverflow.com/questions/20240130/optional-feature-not-implemented-106-sqlbindparameter-error-with-pyodbc
        """
        def safeConvert(value):
            if type(value) is int:
                float_value = float(value)
                if value != int(float_value): #check the exactness of the conversion, since not all integers can be exactly represented as floats
                    raise ValueError("Cannot exactly represent integer: {} as floating point value"
                                     .format(value))
                return float_value
            else:
               return value

        if not isinstance(params, Iterable):
            return safeConvert(params)
        params = [safeConvert(x) for x in params]
        return params

    def ExecuteQuery(self, query, params=None):
        if self.log_enabled:
            logging.info("{0}\n{1}".format(self.path, query))
        cursor = self.connection.cursor()
        try:
            params = self._floatifyIntParams(params)
            cursor.execute(query, params) if params else cursor.execute(query)
        except ProgrammingError as e:
            logging.info("{0}".format(query))
            raise
        cursor.commit()

    def ExecuteMany(self, query, params):
        cursor = self.connection.cursor()
        try:
            if not isinstance(params, Iterable):
                params = self._floatifyIntParams(params)
            params = [self._floatifyIntParams(p) for p in params]
            cursor.executemany(query, params)
        except ProgrammingError as e:
            logging.info("{}".format(query))
            raise
        cursor.commit()

    def tableExists(self, tableName):
        """
        check if the specified table name matches the name of an existing
        table in the database
        @param tableName the string to evaluate
        """
        cursor = self.connection.cursor()
        for row in cursor.tables():
            if row.table_name.lower() == tableName.lower():
                return True
        return False


    def Query(self, query, params=None):
        if self.log_enabled:
            logging.info("{0}\n{1}".format(self.path, query))
        cursor = self.connection.cursor()
        params = self._floatifyIntParams(params)
        cursor.execute(query, params) if params else cursor.execute(query)
        return cursor


    def GetMaxID(self, table, IDcolumn):
        """
        get the maximum value of a numeric column for the specified table and column names
        """
        result = self.Query("SELECT Max({0}.{1}) AS MaxID FROM {0};"
                            .format(table, IDcolumn)).fetchone()
        
        if result is None or result[0] is None: #garbage
            return 0
        return result.MaxID


    def filenameWithoutExtension(self):
        return os.path.splitext(self.filenameWithoutPath())[0]


    def filenameWithoutPath(self):
        name_tokens = os.path.split(self.path)
        return name_tokens[len(name_tokens) - 1]


    def dirname(self):
        return os.path.dirname(self.path)

    def get_batched_query_ranges(self, table_name, id_colname,
                                     max_batch_size):
        """
        workaround for "File sharing lock count exceeded.../ MaxLocksPerFile"
        issues that can occur in access database queries that affect a large 
        number of rows in a single table. This can happen, for example, when 
        deleting all rows, or updating all rows in a table.
        @param table_name the name of the table the query is affecting
        @param id_colname the name of an unique-constrained integer column 
               in the specified table which is used to limit the number of 
               rows affected
        @param max_batch_size the maximum number of rows to affect per batch
        @returns list of tuples representing (min,max) bounds to apply to query
                 batches
        """
        max_id = self.GetMaxID(table_name, id_colname)
        n_batches = int(max_id/max_batch_size)
        remainder = max_id % max_batch_size
        ranges = [(x*max_batch_size,
                   x*max_batch_size+max_batch_size)
                  for x in range(n_batches)]
        if remainder > 0:
            ranges.append((n_batches * max_batch_size,
                           n_batches * max_batch_size + remainder))
        return ranges
