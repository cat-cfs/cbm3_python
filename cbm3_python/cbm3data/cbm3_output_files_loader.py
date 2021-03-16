from types import SimpleNamespace
import pandas as pd
import pyodbc
from cbm3_python.cbm3data import cbm3_output_files


def query_access_db(path, query):
    """Return the result as a dataframe the specified query
    on the access database located at the specified path."""
    connection_string = \
        "Driver={Microsoft Access Driver (*.mdb, *.accdb)};" \
        f"User Id='admin';Dbq={path}"

    with pyodbc.connect(connection_string) as connection:
        return pd.read_sql(query, connection)


def load_archive_index_data(aidb_path):
    # load default spu data from archive index
    return SimpleNamespace(
        tblSPUDefault=query_access_db(
            aidb_path, "SELECT * FROM tblSPUDefault"),
        tblEcoBoundaryDefault=query_access_db(
            aidb_path, "SELECT * FROM tblEcoBoundaryDefault"),
        tblAdminBoundaryDefault=query_access_db(
            aidb_path, "SELECT * FROM tblAdminBoundaryDefault"),
        tblDisturbanceTypeDefault=query_access_db(
            aidb_path, "SELECT * FROM tblDisturbanceTypeDefault"),
        tblUNFCCCLandClass=query_access_db(
            aidb_path, "SELECT * FROM tblUNFCCCLandClass"))


def create_default_data_views(aidb_data):
    default_spu_view = aidb_data.tblSPUDefault[
        ["SPUID", "AdminBoundaryID", "EcoBoundaryID"]].merge(
        aidb_data.tblEcoBoundaryDefault[
            ["EcoBoundaryID", "EcoBoundaryName"]]).merge(
        aidb_data.tblAdminBoundaryDefault[
            ["AdminBoundaryID", "AdminBoundaryName"]])
    default_spu_view = default_spu_view.rename(columns={
        "SPUID": "DefaultSPUID",
        "AdminBoundaryID": "DefaultAdminBoundaryID",
        "EcoBoundaryID": "DefaultEcoBoundaryID",
        "EcoBoundaryName": "DefaultEcoBoundaryName",
        "AdminBoundaryName": "DefaultAdminBoundaryName"})

    unfccc_land_class_view = aidb_data.tblUNFCCCLandClass.rename(
        columns={"Name": "UNFCCCLandClassName"})

    default_disturbance_type_view = aidb_data.tblDisturbanceTypeDefault[
        ["DistTypeID", "DistTypeName", "Description"]
    ]

    default_disturbance_type_view = default_disturbance_type_view.rename(
        columns={
            "DistTypeID": "DefaultDistTypeID",
            "DistTypeName": "DefaultDistTypeName",
            "Description": "DefaultDistTypeDescription"
        })
    return SimpleNamespace(
        unfccc_land_class_view=unfccc_land_class_view,
        default_disturbance_type_view=default_disturbance_type_view,
        default_spu_view=default_spu_view)


def load_project_level_data(project_db_path):
    # get project level info
    return SimpleNamespace(
        tblSPU=query_access_db(
            project_db_path, "SELECT * FROM tblSPU"),
        tblEcoBoundary=query_access_db(
            project_db_path, "SELECT * FROM tblEcoBoundary"),
        tblAdminBoundary=query_access_db(
            project_db_path, "SELECT * FROM tblAdminBoundary"),
        tblDisturbanceType=query_access_db(
            project_db_path, "SELECT * FROM tblDisturbanceType"),
        tblClassifiers=query_access_db(
            project_db_path, "SELECT * FROM tblClassifiers"),
        tblClassifierValues=query_access_db(
            project_db_path, "SELECT * FROM tblClassifierValues"),
        tblClassifierSetValues=query_access_db(
            project_db_path, "SELECT * FROM tblClassifierSetValues"))


def create_project_data_view(project_data, default_view):
    project_spu_view = project_data.tblSPU[
        ["SPUID", "AdminBoundaryID", "EcoBoundaryID", "DefaultSPUID"]
    ].merge(
        project_data.tblEcoBoundary[
            ["EcoBoundaryID", "EcoBoundaryName"]]).merge(
        project_data.tblAdminBoundary[
            ["AdminBoundaryID", "AdminBoundaryName"]])

    project_spu_view = project_spu_view.rename(
        columns={
            "SPUID": "ProjectSPUID",
            "EcoBoundaryID": "ProjectEcoBoundaryID",
            "EcoBoundaryName": "ProjectEcoBoundaryName",
            "AdminBoundaryID": "ProjectAdminBoundaryID",
            "AdminBoundaryName": "ProjectAdminBoundaryName"})

    # re-order colums
    project_spu_view = project_spu_view[[
        "ProjectSPUID", "ProjectAdminBoundaryID",
        "ProjectEcoBoundaryID", "ProjectEcoBoundaryName",
        "ProjectAdminBoundaryName", "DefaultSPUID"]]

    project_spu_view = project_spu_view.merge(
        default_view.default_spu_view)

    disturbance_type_view = project_data.tblDisturbanceType[
        ["DistTypeID", "DistTypeName", "Description", "DefaultDistTypeID"]]
    disturbance_type_view = disturbance_type_view.merge(
        default_view.default_disturbance_type_view)
    disturbance_type_view = disturbance_type_view.rename(columns={
        "DistTypeID": "ProjectDistTypeID",
        "DistTypeName": "ProjectDistTypeName",
        "Description": "ProjectDistTypeDescription"
    })


def create_loaded_classifiers(tblClassifiers, tblClassifierSetValues,
                              cbm_results_dir, chunksize=None):

    if chunksize:
        raise ValueError("chunksize not yet supported")
    raw_results_tables = [
        cbm3_output_files.load_age_indicators(
            cbm_results_dir, chunksize),
        cbm3_output_files.load_dist_indicators(
            cbm_results_dir, chunksize),
        cbm3_output_files.load_flux_indicators(
            cbm_results_dir, chunksize),
        cbm3_output_files.load_pool_indicators(
            cbm_results_dir, chunksize)]

    raw_cset_columns = [f"c{x+1}" for x in range(0, len(tblClassifiers.index))]
    cset_pivot = tblClassifierSetValues.pivot(
        index="ClassifierSetID", columns="ClassifierID")
    cset_pivot = cset_pivot.rename_axis(None)
    cset_pivot.columns = raw_cset_columns
    cset_pivot = cset_pivot.reset_index()
    cset_pivot = cset_pivot.rename(columns={"index": "ClassifierSetID"})

    raw_classifier_data = pd.DataFrame()
    for table in raw_results_tables:
        raw_classifier_data = raw_classifier_data.append(
            table[raw_cset_columns]).drop_duplicates()

    missing_csets = cset_pivot.merge(
        raw_classifier_data, how="right").reset_index(drop=True)
    missing_csets = missing_csets[missing_csets.ClassifierSetID.isna()]

    missing_csets.ClassifierSetID = \
        missing_csets.index + cset_pivot.ClassifierSetID.max() + 1
    cset_pivot = cset_pivot.append(missing_csets)

    return cset_pivot


def replace_with_classifier_set_id(raw_table, cset_pivot):
    classifier_set_insertion_index = raw_table.columns.get_loc("c1")
    raw_cset_columns = [f"c{x+1}" for x in range(0, len(cset_pivot.columns)-1)]
    raw_table = cset_pivot.merge(
        raw_table, left_on=raw_cset_columns,
        right_on=raw_cset_columns, copy=True)
    classifier_set_col = raw_table["ClassifierSetID"]
    raw_table = raw_table.drop(columns=[f"c{x}" for x in range(1, 11)])
    raw_table = raw_table[list(raw_table.columns)[1:]]
    raw_table.insert(
        loc=classifier_set_insertion_index, column="UserDefdClassSetID",
        value=classifier_set_col)
    return raw_table


def melt_loaded_csets(csets):
    csets_melt = csets.copy()
    csets_melt.columns = \
        [csets_melt.columns[0]] + list(range(1, len(csets_melt.columns)))
    csets_melt = pd.melt(csets_melt, id_vars=["ClassifierSetID"])
    csets_melt = csets_melt.rename(columns={
        "variable": "ClassifierID",
        "value": "ClassifierValueID"
    }).sort_values(by=["ClassifierSetID", "ClassifierID"])
    return csets_melt


def process_output_tables(loaded_csets, pool_indicators, flux_indicators,
                          age_indicators, dist_indicators):

    pool_indicators_new = replace_with_classifier_set_id(
        pool_indicators, loaded_csets)
    flux_indicators_new = replace_with_classifier_set_id(
        flux_indicators, loaded_csets)
    age_indicators_new = replace_with_classifier_set_id(
        age_indicators, loaded_csets)
    dist_indicators_new = replace_with_classifier_set_id(
        dist_indicators, loaded_csets)
    data = [
        # TODO: store this information in package
        (pool_indicators_new, "pool_indicators_column_mapping.csv"),
        (flux_indicators_new, "flux_indicators_column_mapping.csv"),
        (age_indicators_new, "age_indicators_column_mapping.csv"),
        (dist_indicators_new, "dist_indicators_column_mapping.csv")]
    for table, csv_path in data:
        mapping_data = pd.read_csv(csv_path)
        column_map = {
            row.CBM3_raw_column_name: row.RRDB_column_name
            for _, row in mapping_data.dropna().iterrows()}
        table.rename(columns=column_map, inplace=True)

    pool_indicators_new.drop(columns="RunID", inplace=True)
    pool_indicators_new.insert(
        loc=0, column="PoolIndID", value=pool_indicators_new.index+1)

    flux_indicators_new.drop(columns="RunID", inplace=True)
    flux_indicators_new.insert(
        loc=0, column="FluxIndicatorID", value=flux_indicators_new.index+1)
    flux_indicators_new["GrossGrowth_AG"] = flux_indicators_new[
        ["DeltaBiomass_AG", "MerchLitterInput", "FolLitterInput",
         "OthLitterInput", "SubMerchLitterInput"]].sum(axis=1)
    flux_indicators_new.loc[
        flux_indicators_new.DistTypeID != 0, "GrossGrowth_AG"] = 0.0
    flux_indicators_new["GrossGrowth_BG"] = flux_indicators_new[
        ["DeltaBiomass_BG", "CoarseLitterInput", "FineLitterInput"]
    ].sum(axis=1)
    flux_indicators_new.loc[
        flux_indicators_new.DistTypeID != 0, "GrossGrowth_BG"] = 0.0
    return SimpleNamespace(
        pool_indicators=pool_indicators_new,
        flux_indicators=flux_indicators_new,
        age_indicators=age_indicators_new,
        dist_indicators=dist_indicators_new)


def load_output_relational_tables(cbm_run_results_dir, project_db_path,
                                  aidb_path, chunksize=None):

    project_data = load_project_level_data(project_db_path)

    aidb_data = load_archive_index_data(aidb_path)

    loaded_csets = create_loaded_classifiers(
        project_data.tblClassifiers,
        project_data.tblClassifierSetValues,
        cbm_run_results_dir, chunksize=chunksize)
    project_data.tblClassifierSetValues = loaded_csets
    results = load_results(
        loaded_csets, cbm_run_results_dir, chunksize)

    return SimpleNamespace(
        **results.__dict__, **project_data.__dict__, **aidb_data.__dict__)


def load_output_descriptive_tables(cbm_run_results_dir, project_db_path,
                                   aidb_path, chunksize=None):
    project_data = load_project_level_data(project_db_path)
    aidb_data = load_archive_index_data(aidb_path)
    default_view = create_default_data_views(aidb_data)
    project_view = create_project_data_view(project_data, default_view)
    loaded_csets = create_loaded_classifiers(
        project_data.tblClassifiers,
        project_data.tblClassifierSetValues,
        cbm_run_results_dir, chunksize=chunksize)
    results = load_results(loaded_csets, cbm_run_results_dir, chunksize)
    # TODO: merge project_view with results tables
    return results


def load_results(loaded_csets, cbm_run_results_dir, project_db_path,
                 chunksize=None):
    if chunksize:
        raise ValueError("not yet supported.")

    age_indicators = cbm3_output_files.load_age_indicators(
        cbm_run_results_dir, chunksize)
    dist_indicators = cbm3_output_files.load_dist_indicators(
        cbm_run_results_dir, chunksize)
    flux_indicators = cbm3_output_files.load_flux_indicators(
        cbm_run_results_dir, chunksize)
    pool_indicators = cbm3_output_files.load_pool_indicators(
        cbm_run_results_dir, chunksize)

    result = process_output_tables(
        loaded_csets, pool_indicators, flux_indicators, age_indicators,
        dist_indicators)

    return result
