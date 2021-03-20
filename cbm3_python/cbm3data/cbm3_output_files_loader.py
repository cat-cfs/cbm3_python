import os
import pandas as pd

from cbm3_python.cbm3data import cbm3_output_files
from cbm3_python.cbm3data import cbm3_output_descriptions
from cbm3_python.cbm3data.cbm3_output_descriptions import ResultsDescriber


def _make_iterable(func, *args, **kwargs):
    result = func(*args, **kwargs)
    if hasattr(result, "__iter__"):
        return result
    else:
        return [result]


def _create_loaded_classifiers(tblClassifiers, tblClassifierSetValues,
                               cbm_results_dir, chunksize=None):

    raw_cset_columns = [f"c{x+1}" for x in range(0, len(tblClassifiers.index))]
    cset_pivot = tblClassifierSetValues.pivot(
        index="ClassifierSetID", columns="ClassifierID")
    cset_pivot = cset_pivot.rename_axis(None)
    cset_pivot.columns = raw_cset_columns
    cset_pivot = cset_pivot.reset_index()
    cset_pivot = cset_pivot.rename(columns={"index": "ClassifierSetID"})

    raw_classifier_data = pd.DataFrame()

    pool_indicators = _make_iterable(
        cbm3_output_files.load_pool_indicators(cbm_results_dir, chunksize))
    for chunk in pool_indicators:
        raw_classifier_data = raw_classifier_data.append(
            chunk[raw_cset_columns]).drop_duplicates()

    missing_csets = cset_pivot.merge(
        raw_classifier_data, how="right").reset_index(drop=True)
    missing_csets = missing_csets[missing_csets.ClassifierSetID.isna()]

    missing_csets.ClassifierSetID = \
        missing_csets.index + cset_pivot.ClassifierSetID.max() + 1
    cset_pivot = cset_pivot.append(missing_csets)

    return cset_pivot


def _replace_with_classifier_set_id(raw_table, cset_pivot):
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


def _melt_loaded_csets(csets):
    csets_melt = csets.copy()
    csets_melt.columns = \
        [csets_melt.columns[0]] + list(range(1, len(csets_melt.columns)))
    csets_melt = pd.melt(csets_melt, id_vars=["ClassifierSetID"])
    csets_melt = csets_melt.rename(columns={
        "variable": "ClassifierID",
        "value": "ClassifierValueID"
    }).sort_values(by=["ClassifierSetID", "ClassifierID"])
    return csets_melt


def _get_local_file(filename):
    return os.path.join(
        os.path.dirname(os.path.realpath(__file__)), filename)


def _process_pool_indicator_table(loaded_csets, pool_indicators):
    pool_indicators_new = _replace_with_classifier_set_id(
        pool_indicators, loaded_csets)
    mapping_data = pd.read_csv(
        _get_local_file("pool_indicators_column_mapping.csv"))
    column_map = {
        row.CBM3_raw_column_name: row.RRDB_column_name
        for _, row in mapping_data.dropna().iterrows()}
    pool_indicators_new.rename(columns=column_map, inplace=True)
    pool_indicators_new.drop(columns="RunID", inplace=True)
    pool_indicators_new.insert(
        loc=0, column="PoolIndID", value=pool_indicators_new.index+1)
    return pool_indicators_new


def _process_flux_indicator_table(loaded_csets, flux_indicators):
    flux_indicators_new = _replace_with_classifier_set_id(
        flux_indicators, loaded_csets)
    mapping_data = pd.read_csv(
        _get_local_file("flux_indicators_column_mapping.csv"))
    column_map = {
        row.CBM3_raw_column_name: row.RRDB_column_name
        for _, row in mapping_data.dropna().iterrows()}
    flux_indicators_new.rename(columns=column_map, inplace=True)
    flux_indicators_new.drop(columns="RunID", inplace=True)
    flux_indicators_new.insert(
        loc=0, column="FluxIndicatorID", value=flux_indicators_new.index+1)
    # gross growth AG and BG are composite flux indicators that are not
    # included in RAW CBM3 output, but are present in tblFluxIndicators
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


def _process_age_indicator_table(loaded_csets, age_indicators):
    age_indicators_new = _replace_with_classifier_set_id(
        age_indicators, loaded_csets)
    mapping_data = pd.read_csv(
        _get_local_file("age_indicators_column_mapping.csv"))
    column_map = {
        row.CBM3_raw_column_name: row.RRDB_column_name
        for _, row in mapping_data.dropna().iterrows()}
    age_indicators_new.rename(columns=column_map, inplace=True)
    age_indicators_new.drop(columns="RunID", inplace=True)
    age_indicators_new.insert(
        loc=0, column="AgeIndID", value=age_indicators_new.index+1)
    return age_indicators_new


def _process_dist_indicator_table(loaded_csets, dist_indicators):
    dist_indicators_new = _replace_with_classifier_set_id(
        dist_indicators, loaded_csets)
    mapping_data = pd.read_csv(
        _get_local_file("dist_indicators_column_mapping.csv"))
    column_map = {
        row.CBM3_raw_column_name: row.RRDB_column_name
        for _, row in mapping_data.dropna().iterrows()}
    dist_indicators_new.rename(columns=column_map, inplace=True)
    dist_indicators_new.drop(columns="RunID", inplace=True)
    dist_indicators_new.insert(
        loc=0, column="DistIndID", value=dist_indicators_new.index+1)
    return dist_indicators_new


def load_output_relational_tables(cbm_run_results_dir, project_db_path,
                                  aidb_path, out_func, chunksize=None):

    project_data = cbm3_output_descriptions.load_project_level_data(
        project_db_path)

    aidb_data = cbm3_output_descriptions.load_archive_index_data(aidb_path)

    loaded_csets = _create_loaded_classifiers(
        project_data.tblClassifiers,
        project_data.tblClassifierSetValues,
        cbm_run_results_dir, chunksize=chunksize)
    project_data.tblClassifierSetValues = _melt_loaded_csets(loaded_csets)

    for k, v in aidb_data.__dict__.items():
        out_func(k, v)
    for k, v in project_data.__dict__.items():
        out_func(k, v)

    results_list = [
        {"table_name": "tblAgeIndicators",
         "load_function": cbm3_output_files.load_age_indicators,
         "process_function": _process_age_indicator_table},
        {"table_name": "tblDistIndicators",
         "load_function": cbm3_output_files.load_dist_indicators,
         "process_function": _process_dist_indicator_table},
        {"table_name": "tblPoolIndicators",
         "load_function": cbm3_output_files.load_pool_indicators,
         "process_function": _process_pool_indicator_table},
        {"table_name": "tblFluxIndicators",
         "load_function": cbm3_output_files.load_flux_indicators,
         "process_function": _process_flux_indicator_table}]

    for result_item in results_list:
        result_chunk_iterable = _make_iterable(
            result_item["load_function"](cbm_run_results_dir, chunksize))
        for chunk in result_chunk_iterable:
            out_func(
                result_item["table_name"],
                result_item["process_function"](loaded_csets, chunk))


def load_output_descriptive_tables(cbm_run_results_dir, project_db_path,
                                   aidb_path, out_func,
                                   classifier_value_field="Name",
                                   chunksize=None):
    project_data = cbm3_output_descriptions.load_project_level_data(
        project_db_path)
    loaded_csets = _create_loaded_classifiers(
        project_data.tblClassifiers,
        project_data.tblClassifierSetValues,
        cbm_run_results_dir, chunksize=chunksize)
    d = ResultsDescriber(
        project_db_path, aidb_path, loaded_csets, classifier_value_field)
    results_list = [
        {"table_name": "tblAgeIndicators",
         "load_function": cbm3_output_files.load_age_indicators,
         "process_function": _process_age_indicator_table,
         "describe_function": d.merge_age_indicator_descriptions},
        {"table_name": "tblDistIndicators",
         "load_function": cbm3_output_files.load_dist_indicators,
         "process_function": _process_dist_indicator_table,
         "describe_function": d.merge_age_indicator_descriptions},
        {"table_name": "tblPoolIndicators",
         "load_function": cbm3_output_files.load_pool_indicators,
         "process_function": _process_pool_indicator_table,
         "describe_function": d.merge_age_indicator_descriptions},
        {"table_name": "tblFluxIndicators",
         "load_function": cbm3_output_files.load_flux_indicators,
         "process_function": _process_flux_indicator_table,
         "describe_function": d.merge_age_indicator_descriptions}]

    for result_item in results_list:
        result_chunk_iterable = _make_iterable(
            result_item["load_function"](cbm_run_results_dir, chunksize))
        for chunk in result_chunk_iterable:
            processed_chunk = result_item["process_function"](
                loaded_csets, chunk)
            described_chunk = result_item["describe_function"](processed_chunk)
            out_func(result_item["table_name"], described_chunk)
