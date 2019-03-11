import operator
from enum import Enum
import pandas as pd
from collections import OrderedDict
from cbm3_python.cbm3data.accessdb import AccessDB
from cbm3_python.cbm3data import results_queries


operator_lookup = {
    "<":  operator.lt,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne,
    ">=": operator.ge,
    ">": operator.gt
    }



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





def pivot(df, group_col, pivot_col):

    unique_values = df[pivot_col].unique()
    outList = []
    for item in unique_values:
        subset = df.loc[df[pivot_col]==item].groupby(group_col).sum()
        pivot_headers = ["{pivot_col}: {pivot_val} {variable}"
                         .format(
                            pivot_col=pivot_col,
                            pivot_val=item,
                            variable=x) for x in subset.columns.values]
        subset.columns = pivot_headers
        outList.append(subset)
    output = pd.concat(outList, axis=1)
    return output


def load_pool_indicators(results_db_path, 
        spatial_unit_grouping=False,
        classifier_set_grouping=False,
        land_class_grouping=False):
    sql = results_queries.get_pool_indicators_view_sql(
        spatial_unit_grouping, classifier_set_grouping, land_class_grouping)
    if classifier_set_grouping:
        df  = as_data_frame(sql, results_db_path)
        return join_classifiers(df, get_classifier_values(results_db_path))
    else:
        return as_data_frame(sql, results_db_path)

def join_classifiers(indicators, classifiers):
    df = pd.merge(
        indicators, classifiers,
        left_on ="UserDefdClassSetID",
        right_on="UserDefdClassSetID")
    return df

def create_filter(column, func, value):
    return lambda df : df.loc[operator_lookup[func](df[column], value)]

def filter(indicators, filters=None):
    if filters is None:
        return
    for f in filters:
        df = f(df)

def as_data_frame(query, results_db_path):
    with AccessDB(results_db_path) as results_db:
        df = pd.read_sql(query, results_db.connection)
    return df