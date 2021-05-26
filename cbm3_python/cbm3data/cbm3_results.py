import pandas as pd

from cbm3_python.cbm3data import results_queries
from cbm3_python.cbm3data.results_queries import stock_changes_view

def _load_df(sql, results_db):
    from cbm3_python.cbm3data import accessdb
    if isinstance(results_db, str):
        return accessdb.as_data_frame(sql, results_db)
    else:
        return pd.read_sql(sql, results_db)


def get_classifier_values(results_db):
    '''
    loads the classifier values in the specified results database into an
    indexed collection to serve for labels, grouping and filtering CBM results
    tables
    '''
    sql = results_queries.get_classifiers_view()
    df = _load_df(sql, results_db)
    return df.pivot(
        index="UserDefdClassSetID", columns="ClassDesc",
        values="UserDefdSubClassName")


def load_pool_indicators(results_db,
                         spatial_unit_grouping=False,
                         classifier_set_grouping=False,
                         land_class_grouping=False,
                         rollup_format=False):
    sql = results_queries.get_pool_indicators_view_sql(
        spatial_unit_grouping, classifier_set_grouping, land_class_grouping)
    df = _load_df(sql, results_db)
    if classifier_set_grouping:
        df = _join_classifiers(df, get_classifier_values(results_db))
    if spatial_unit_grouping:
        df = _join_spatial_units(df, _load_df(
            results_queries.get_spatial_units_view(rollup_format),
            results_db))
    return df


def load_stock_changes(results_db,
                       disturbance_type_grouping=False,
                       spatial_unit_grouping=False,
                       classifier_set_grouping=False,
                       land_class_grouping=False,
                       rollup_format=False):
    flux_ind_df = load_flux_indicators(
        results_db, disturbance_type_grouping, spatial_unit_grouping,
        classifier_set_grouping, land_class_grouping, rollup_format)
    return stock_changes_view.get_stock_changes_view(flux_ind_df)


def load_flux_indicators(results_db,
                         disturbance_type_grouping=False,
                         spatial_unit_grouping=False,
                         classifier_set_grouping=False,
                         land_class_grouping=False,
                         rollup_format=False):
    sql = results_queries.get_flux_indicators_view(
        disturbance_type_grouping, spatial_unit_grouping,
        classifier_set_grouping, land_class_grouping)
    df = _load_df(sql, results_db)
    if classifier_set_grouping:
        df = _join_classifiers(df, get_classifier_values(results_db))
    if spatial_unit_grouping:
        df = _join_spatial_units(df, _load_df(
            results_queries.get_spatial_units_view(rollup_format),
            results_db))
    if disturbance_type_grouping:
        df = _join_disturbance_types(df, _load_df(
            results_queries.get_disturbance_types_view(rollup_format),
            results_db))
    return df


def load_age_indicators(results_db,
                        spatial_unit_grouping=False,
                        classifier_set_grouping=False,
                        land_class_grouping=False,
                        rollup_format=False):
    sql = results_queries.get_age_indicators_view_sql(
        spatial_unit_grouping, classifier_set_grouping,
        land_class_grouping)
    df = _load_df(sql, results_db)
    if classifier_set_grouping:
        df = _join_classifiers(df, get_classifier_values(results_db))
    if spatial_unit_grouping:
        df = _join_spatial_units(df, _load_df(
            results_queries.get_spatial_units_view(rollup_format),
            results_db))
    return df


def load_disturbance_indicators(results_db,
                                disturbance_type_grouping=False,
                                spatial_unit_grouping=False,
                                classifier_set_grouping=False,
                                land_class_grouping=False,
                                rollup_format=False):
    sql = results_queries.get_disturbance_indicators_view_sql(
        disturbance_type_grouping, spatial_unit_grouping,
        classifier_set_grouping, land_class_grouping)
    df = _load_df(sql, results_db)
    if classifier_set_grouping:
        df = _join_classifiers(df, get_classifier_values(results_db))
    if spatial_unit_grouping:
        df = _join_spatial_units(df, _load_df(
            results_queries.get_spatial_units_view(rollup_format),
            results_db))
    if disturbance_type_grouping:
        df = _join_disturbance_types(df, _load_df(
            results_queries.get_disturbance_types_view(rollup_format),
            results_db))
    return df


def _join_classifiers(indicators, classifiers):
    return pd.merge(
        indicators, classifiers,
        left_on="UserDefdClassSetID",
        right_on="UserDefdClassSetID")


def _join_spatial_units(indicators, spatial_units):
    return pd.merge(indicators, spatial_units,
                    left_on="SPUID", right_on="SPUID")


def _join_disturbance_types(indicators, disturbance_types):
    return pd.merge(indicators, disturbance_types,
                    left_on="DistTypeID", right_on="DistTypeID")
