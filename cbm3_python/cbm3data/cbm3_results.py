import operator
import pandas as pd
from collections import OrderedDict
from cbm3_python.cbm3data.accessdb import AccessDB
from cbm3_python.cbm3data import results_queries

def get_classifier_values(results_path):
    '''
    loads the classifier values in the specified results database into an
    indexed collection to serve for labels, grouping and filtering CBM results tables
    '''
    sql= results_queries.get_classifiers_view()
    columns = OrderedDict([("UserDefdClassSetID",[])])
    with AccessDB(results_path) as rrdb:
        for row in rrdb.Query(sql):
            if len(columns["UserDefdClassSetID"]) == 0 or \
                columns["UserDefdClassSetID"][-1] != row.UserDefdClassSetID:
                columns["UserDefdClassSetID"].append(row.UserDefdClassSetID)
            if row.ClassDesc in columns:
                columns[row.ClassDesc].append(row.UserDefdSubClassName)
            else:
                columns[row.ClassDesc] = [row.UserDefdSubClassName]
    return pd.DataFrame(columns)

operator_lookup = {
    "<":  operator.lt,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne,
    ">=": operator.ge,
    ">": operator.gt
    }

def create_filter(column, func, value):
    return lambda df : df.loc[operator_lookup[func](df[column], value)]

def get_indicators_view(results_db_path,
                        data_cols,
                        filters=None,
                        spatial_unit_grouping=False,
                        classifier_set_grouping=False,
                        land_class_grouping=False):
    sql = results_queries.get_pool_indicators_view_sql(
        spatial_unit_grouping, classifier_set_grouping, land_class_grouping)

    df = as_data_frame(sql, results_db_path)
    classifiers=None
    if classifier_set_grouping:
        classifiers = get_classifier_values(results_db_path)
    return query_indicators(data_cols, df, ["TimeStep"], classifiers, filters)

def query_indicators(data_cols, indicators_data, groupby, classifiers=None, filters=None):
    #merge classifiers with indicators data
    if not classifiers is None:
        df = pd.merge(
            indicators_data, classifiers,
            left_on ="UserDefdClassSetID",
            right_on="UserDefdClassSetID")
    else:
        df = indicators_data

    if not filters is None:
        for f in filters:
            df = f(df)

    return df[groupby+data_cols].groupby(groupby).sum()

def as_data_frame(query, results_db_path):
    with AccessDB(results_db_path) as results_db:
        df = pd.read_sql(query, results_db.connection)
