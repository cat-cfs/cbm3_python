import os

def get_script_dir():
    return os.path.dirname(os.path.realpath(__file__))

def get_local_path(filename):
    return os.path.join(get_script_dir(), filename)

def get_classifiers_view():
    with open(get_local_path("classifiers_view.sql")) as f:
        return f.read()

def build_groupings(table_name,
    disturbance_type_grouping=False,
    spatial_unit_grouping=False,
    classifier_set_grouping=False,
    land_class_grouping=False):

    if disturbance_type_grouping:
        groupings.append("{table_name}.DistTypeID"
                         .format(table_name=table_name))
    if spatial_unit_grouping:
        groupings.append("{table_name}.SPUID"
                         .format(table_name=table_name))
    if classifier_set_grouping:
        groupings.append("{table_name}.UserDefdClassSetID"
                         .format(table_name=table_name))
    if land_class_grouping:
        groupings.append(",".join(
            ["{table_name}_{col_name}".format(
                table_name=table_name,
                col_name=x)
            for x in ["LandClassID","kf2","kf3","kf4","kf5","kf6"]]))

def get_formatted_query(path, table_name,
                        disturbance_type_grouping=False,
                        spatial_unit_grouping=False,
                        classifier_set_grouping=False,
                        land_class_grouping=False):
    path = get_local_path(path)
    with open(path) as f:
        sql = f.read()

    groupings = build_groupings(
        table_name, disturbance_type_grouping, spatial_unit_grouping,
        classifier_set_grouping, land_class_grouping)

    if len(groupings) > 0:
        sql = sql.format(",".join(groupings) + ",",
                   "," + ",".join(groupings))
    else:
        sql = sql.format("","")
    return sql

def get_stock_changes_view(disturbance_type_grouping=False,
                           spatial_unit_grouping=False,
                           classifier_set_grouping=False,
                           land_class_grouping=False):
    return get_formatted_query(
        get_local_path("stock_changes_view.sql"), "tfi",
        disturbance_type_grouping, spatial_unit_grouping,
        classifier_set_grouping, land_class_grouping)

def get_pool_indicators_view_sql(spatial_unit_grouping=False,
                                 classifier_set_grouping=False,
                                 land_class_grouping=False):
    return get_formatted_query(
        get_local_path("pool_indicators_view.sql"), "tpi",
        False, spatial_unit_grouping,
        classifier_set_grouping, land_class_grouping)

def get_age_indicators_view_sql(spatial_unit_grouping=False,
                                classifier_set_grouping=False,
                                land_class_grouping=False):
    return get_formatted_query(
        get_local_path("age_indicators_view.sql"), "tai",
        False, spatial_unit_grouping, classifier_set_grouping,
        land_class_grouping)

def get_disturbance_indicators_view_sql(disturbance_type_grouping=False,
                                        spatial_unit_grouping=False,
                                        classifier_set_grouping=False,
                                        land_class_grouping=False):
    return get_formatted_query(
        get_local_path("disturbance_indicators_view.sql"), "tdi",
        False, spatial_unit_grouping, classifier_set_grouping,
        land_class_grouping)