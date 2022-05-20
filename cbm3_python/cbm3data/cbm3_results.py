import pandas as pd
import warnings
from cbm3_python.cbm3data import results_queries
from cbm3_python.cbm3data.results_queries import stock_changes_view


def _load_df(sql, results_db):
    from cbm3_python.cbm3data import accessdb

    if isinstance(results_db, str):
        return accessdb.as_data_frame(sql, results_db)
    else:
        return pd.read_sql(sql, results_db)


def _get_classifier_values(results_db):
    """
    loads the classifier values in the specified results database into an
    indexed collection to serve for labels, grouping and filtering CBM results
    tables
    """
    sql = results_queries.get_classifiers_view()
    df = _load_df(sql, results_db)
    return df.pivot(
        index="UserDefdClassSetID",
        columns="ClassDesc",
        values="UserDefdSubClassName",
    )


def load_pool_indicators(
    results_db,
    spatial_unit_grouping=False,
    classifier_set_grouping=False,
    land_class_grouping=False,
    rollup_format=False,
):
    """Load pool indicators from a cbm3 results database

    Args:
        results_db (str, connection, or sqlalchemy.Connectable): path to a
            CBM3 MS access database if a string is specified. Otherwise a
            connection to a database with CBM3 results schema.
        spatial_unit_grouping (bool, optional): If set to True the result will
            be returned with spatial unit stratification. Defaults to False.
        classifier_set_grouping (bool, optional): If set to True the result
            will be returned with classifier set stratification. Defaults to
            False.
        land_class_grouping (bool, optional): If set to True the result will
            be returned with land class stratification. Defaults to False.
        rollup_format (bool, optional): If set to true query the database in
            the rollup CBM3 format variant. Defaults to False.

    Returns:
        pandas.DataFrame: dataframe containing the results
    """
    sql = results_queries.get_pool_indicators_view_sql(
        spatial_unit_grouping, classifier_set_grouping, land_class_grouping
    )
    df = _load_df(sql, results_db)
    if classifier_set_grouping:
        df = _join_classifiers(df, _get_classifier_values(results_db))
    if spatial_unit_grouping:
        df = _join_spatial_units(
            df,
            _load_df(
                results_queries.get_spatial_units_view(rollup_format),
                results_db,
            ),
        )
    return df


def load_stock_changes(
    results_db,
    disturbance_type_grouping=True,
    spatial_unit_grouping=False,
    classifier_set_grouping=False,
    land_class_grouping=False,
    rollup_format=False,
):
    """Load stock changes from a cbm3 results database

    Args:
        results_db (str, connection, or sqlalchemy.Connectable): path to a
            CBM3 MS access database if a string is specified. Otherwise a
            connection to a database with CBM3 results schema.
        disturbance_type_grouping (bool, optional):  If set to True the result
            will be returned with disturbance type stratification. Defaults to
            True. If set to false a warning will be produced but the result
            will still be returned with disturbance type stratification, since
            this is required to compute certain stock changes columns.
        spatial_unit_grouping (bool, optional): If set to True the result will
            be returned with spatial unit stratification. Defaults to False.
        classifier_set_grouping (bool, optional): If set to True the result
            will be returned with classifier set stratification. Defaults to
            False.
        land_class_grouping (bool, optional): If set to True the result will
            be returned with land class stratification. Defaults to False.
        rollup_format (bool, optional): If set to true query the database in
            the rollup CBM3 format variant. Defaults to False.

    Returns:
        pandas.DataFrame: dataframe containing the results
    """
    if not disturbance_type_grouping:
        warnings.warn(
            "disturbance type stratification is required for computing stock "
            "changes. This warning can be avoided by omitting (or setting "
            "true) the `disturbance_type_grouping` parameter in calls to "
            "`cbm3_python.cbm3data.cbm3_results.load_stock_changes` "
        )
    flux_ind_df = load_flux_indicators(
        results_db,
        disturbance_type_grouping=True,
        spatial_unit_grouping=spatial_unit_grouping,
        classifier_set_grouping=classifier_set_grouping,
        land_class_grouping=land_class_grouping,
        rollup_format=rollup_format,
    )
    return stock_changes_view.get_stock_changes_view(flux_ind_df)


def load_flux_indicators(
    results_db,
    disturbance_type_grouping=False,
    spatial_unit_grouping=False,
    classifier_set_grouping=False,
    land_class_grouping=False,
    rollup_format=False,
):
    """Load flux indicators from a cbm3 results database

    Args:
        results_db (str, connection, or sqlalchemy.Connectable): path to a
            CBM3 MS access database if a string is specified. Otherwise a
            connection to a database with CBM3 results schema.
        disturbance_type_grouping (bool, optional):  If set to True the result
            will be returned with disturbance type stratification. Defaults to
            False.
        spatial_unit_grouping (bool, optional): If set to True the result will
            be returned with spatial unit stratification. Defaults to False.
        classifier_set_grouping (bool, optional): If set to True the result
            will be returned with classifier set stratification. Defaults to
            False.
        land_class_grouping (bool, optional): If set to True the result will
            be returned with land class stratification. Defaults to False.
        rollup_format (bool, optional): If set to true query the database in
            the rollup CBM3 format variant. Defaults to False.

    Returns:
        pandas.DataFrame: dataframe containing the results
    """
    sql = results_queries.get_flux_indicators_view(
        disturbance_type_grouping,
        spatial_unit_grouping,
        classifier_set_grouping,
        land_class_grouping,
    )
    df = _load_df(sql, results_db)
    if classifier_set_grouping:
        df = _join_classifiers(df, _get_classifier_values(results_db))
    if spatial_unit_grouping:
        df = _join_spatial_units(
            df,
            _load_df(
                results_queries.get_spatial_units_view(rollup_format),
                results_db,
            ),
        )
    if disturbance_type_grouping:
        df = _join_disturbance_types(
            df,
            _load_df(
                results_queries.get_disturbance_types_view(rollup_format),
                results_db,
            ),
        )
    return df


def load_age_indicators(
    results_db,
    spatial_unit_grouping=False,
    classifier_set_grouping=False,
    land_class_grouping=False,
    rollup_format=False,
):
    """Load age indicators from a cbm3 results database

    Args:
        results_db (str, connection, or sqlalchemy.Connectable): path to a
            CBM3 MS access database if a string is specified. Otherwise a
            connection to a database with CBM3 results schema.
        spatial_unit_grouping (bool, optional): If set to True the result will
            be returned with spatial unit stratification. Defaults to False.
        classifier_set_grouping (bool, optional): If set to True the result
            will be returned with classifier set stratification. Defaults to
            False.
        land_class_grouping (bool, optional): If set to True the result will
            be returned with land class stratification. Defaults to False.
        rollup_format (bool, optional): If set to true query the database in
            the rollup CBM3 format variant. Defaults to False.

    Returns:
        pandas.DataFrame: dataframe containing the results
    """
    sql = results_queries.get_age_indicators_view_sql(
        spatial_unit_grouping, classifier_set_grouping, land_class_grouping
    )
    df = _load_df(sql, results_db)
    if classifier_set_grouping:
        df = _join_classifiers(df, _get_classifier_values(results_db))
    if spatial_unit_grouping:
        df = _join_spatial_units(
            df,
            _load_df(
                results_queries.get_spatial_units_view(rollup_format),
                results_db,
            ),
        )
    return df


def load_disturbance_indicators(
    results_db,
    disturbance_type_grouping=False,
    spatial_unit_grouping=False,
    classifier_set_grouping=False,
    land_class_grouping=False,
    rollup_format=False,
):
    """Load disturbance indicators from a cbm3 results database

    Args:
        results_db (str, connection, or sqlalchemy.Connectable): path to a
            CBM3 MS access database if a string is specified. Otherwise a
            connection to a database with CBM3 results schema.
        disturbance_type_grouping (bool, optional):  If set to True the result
            will be returned with disturbance type stratification. Defaults to
            False.
        spatial_unit_grouping (bool, optional): If set to True the result will
            be returned with spatial unit stratification. Defaults to False.
        classifier_set_grouping (bool, optional): If set to True the result
            will be returned with classifier set stratification. Defaults to
            False.
        land_class_grouping (bool, optional): If set to True the result will
            be returned with land class stratification. Defaults to False.
        rollup_format (bool, optional): If set to true query the database in
            the rollup CBM3 format variant. Defaults to False.

    Returns:
        pandas.DataFrame: dataframe containing the results
    """
    sql = results_queries.get_disturbance_indicators_view_sql(
        disturbance_type_grouping,
        spatial_unit_grouping,
        classifier_set_grouping,
        land_class_grouping,
    )
    df = _load_df(sql, results_db)
    if classifier_set_grouping:
        df = _join_classifiers(df, _get_classifier_values(results_db))
    if spatial_unit_grouping:
        df = _join_spatial_units(
            df,
            _load_df(
                results_queries.get_spatial_units_view(rollup_format),
                results_db,
            ),
        )
    if disturbance_type_grouping:
        df = _join_disturbance_types(
            df,
            _load_df(
                results_queries.get_disturbance_types_view(rollup_format),
                results_db,
            ),
        )
    return df


def _join_classifiers(indicators, classifiers):
    return pd.merge(
        classifiers,
        indicators,
        left_on="UserDefdClassSetID",
        right_on="UserDefdClassSetID",
        validate="1:m",
    )


def _join_spatial_units(indicators, spatial_units):
    return pd.merge(
        spatial_units,
        indicators,
        left_on="SPUID",
        right_on="SPUID",
        validate="1:m",
    )


def _join_disturbance_types(indicators, disturbance_types):
    return pd.merge(
        disturbance_types,
        indicators,
        left_on="DistTypeID",
        right_on="DistTypeID",
        validate="1:m",
    )
