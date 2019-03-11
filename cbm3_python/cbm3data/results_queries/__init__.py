import os

def get_script_dir():
    return os.path.dirname(os.path.realpath(__file__))

def get_local_path(filename):
    return os.path.join(get_script_dir(), filename)

def get_classifiers_view():
    with open(get_local_path("classifiers_view.sql")) as f:
        return f.read()

def get_pool_indicators_view_sql(spatial_unit_grouping=False,
                                 classifier_set_grouping=False,
                                 land_class_grouping=False):
    path = get_local_path("pool_indicators_view.sql")
    with open(path) as f:
        sql = f.read()

    groupings = []
    if spatial_unit_grouping:
        groupings.append("tpi.SPUID")
    if classifier_set_grouping:
        groupings.append("tpi.UserDefdClassSetID")
    if land_class_grouping:
        groupings.append(",".join(["tpi.LandClassID","tpi.kf2","tpi.kf3","tpi.kf4","tpi.kf5","tpi.kf6"]))

    if len(groupings) > 0:
        sql = sql.format(",".join(groupings) + ",",
                   "," + ",".join(groupings))
    else:
        sql = sql.format("","")
    return sql