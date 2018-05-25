import pyodbc
import logging
import os
from pyodbc import ProgrammingError

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

        
    def ExecuteQuery(self, query, params=None):
        if self.log_enabled:
            logging.info("{0}\n{1}".format(self.path, query))
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params) if params else cursor.execute(query)
        except ProgrammingError as e:
            logging.info("{0}".format(query))
            raise
        cursor.commit()

    def ExecuteMany(self, query, params):
        cursor = self.connection.cursor()
        try:
            cursor.executemany(query, params)
        except ProgrammingError as e:
            logging.info("{}".format(query))
            raise
        cursor.commit()

    def tableExists(self, tableName):
        cursor = self.connection.cursor()
        for row in cursor.tables():
            if row.table_name.lower() == tableName.lower():
                return True
        return False


    def Query(self, query, params=None):
        if self.log_enabled:
            logging.info("{0}\n{1}".format(self.path, query))
        cursor = self.connection.cursor()
        cursor.execute(query, params) if params else cursor.execute(query)
        return cursor


    def GetMaxID(self, table, IDcolumn):
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
