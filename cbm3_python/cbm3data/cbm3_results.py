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
    df  = as_data_frame(sql, results_path)
    return df.pivot(
        index="UserDefdClassSetID", columns="ClassDesc",
        values="UserDefdSubClassName")


def pivot(df, group_col, pivot_col):

    unique_values = df[pivot_col].unique()
    value_cols = set(list(df)) - set([pivot_col])
    outList = []
    for item in unique_values:
        subset = df.loc[df[pivot_col]==item][value_cols].groupby(group_col).sum()
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
    df  = as_data_frame(sql, results_db_path)
    if classifier_set_grouping:
        df = join_classifiers(df, get_classifier_values(results_db_path))
    if spatial_unit_grouping:
        df = join_spatial_units(df, as_data_frame(
            results_queries.get_spatial_units_view(),
            results_db_path))
    return df


def load_stock_changes(results_db_path,
        disturbance_type_grouping=False,
        spatial_unit_grouping=False,
        classifier_set_grouping=False,
        land_class_grouping=False):
    sql = results_queries.get_stock_changes_view(
        disturbance_type_grouping, spatial_unit_grouping,
        classifier_set_grouping, land_class_grouping)
    df  = as_data_frame(sql, results_db_path)
    if classifier_set_grouping:
        df = join_classifiers(df, get_classifier_values(results_db_path))
    if spatial_unit_grouping:
        df = join_spatial_units(df, as_data_frame(
            results_queries.get_spatial_units_view(),
            results_db_path))
    if disturbance_type_grouping:
        df = join_disturbance_types(df, as_data_frame(
            results_queries.get_disturbance_types_view(),
            results_db_path))
    return df

def load_flux_indicators(results_db_path,
        disturbance_type_grouping=False,
        spatial_unit_grouping=False,
        classifier_set_grouping=False,
        land_class_grouping=False):
    sql = results_queries.get_flux_indicators_view(
        disturbance_type_grouping, spatial_unit_grouping,
        classifier_set_grouping, land_class_grouping)
    df  = as_data_frame(sql, results_db_path)
    if classifier_set_grouping:
        df = join_classifiers(df, get_classifier_values(results_db_path))
    if spatial_unit_grouping:
        df = join_spatial_units(df, as_data_frame(
            results_queries.get_spatial_units_view(),
            results_db_path))
    if disturbance_type_grouping:
        df = join_disturbance_types(df, as_data_frame(
            results_queries.get_disturbance_types_view(),
            results_db_path))
    return df


def load_age_indicators(results_db_path,
        spatial_unit_grouping=False,
        classifier_set_grouping=False,
        land_class_grouping=False):
    sql = results_queries.get_age_indicators_view_sql(
        spatial_unit_grouping, classifier_set_grouping,
        land_class_grouping)
    df  = as_data_frame(sql, results_db_path)
    if classifier_set_grouping:
        df = join_classifiers(df, get_classifier_values(results_db_path))
    if spatial_unit_grouping:
        df = join_spatial_units(df, as_data_frame(
            results_queries.get_spatial_units_view(),
            results_db_path))
    return df


def load_disturbance_indicators(results_db_path,
        disturbance_type_grouping=False,
        spatial_unit_grouping=False,
        classifier_set_grouping=False,
        land_class_grouping=False):
    sql = results_queries.get_disturbance_indicators_view_sql(
        disturbance_type_grouping, spatial_unit_grouping,
        classifier_set_grouping, land_class_grouping)
    df  = as_data_frame(sql, results_db_path)
    if classifier_set_grouping:
        df = join_classifiers(df, get_classifier_values(results_db_path))
    if spatial_unit_grouping:
        df = join_spatial_units(df, as_data_frame(
            results_queries.get_spatial_units_view(),
            results_db_path))
    if disturbance_type_grouping:
        df = join_disturbance_types(df, as_data_frame(
            results_queries.get_disturbance_types_view(),
            results_db_path))
    return df

def join_classifiers(indicators, classifiers):
    return pd.merge(
        indicators, classifiers,
        left_on ="UserDefdClassSetID",
        right_on="UserDefdClassSetID")

def join_spatial_units(indicators, spatial_units):
    return pd.merge(indicators, spatial_units,
                    left_on="SPUID", right_on="SPUID")

def join_disturbance_types(indicators, disturbance_types):
    return pd.merge(indicators, disturbance_types,
                    left_on="DistTypeID", right_on="DistTypeID")

def as_data_frame(query, results_db_path):
    with AccessDB(results_db_path) as results_db:
        df = pd.read_sql(query, results_db.connection)
    return df