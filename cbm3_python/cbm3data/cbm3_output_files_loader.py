import os
import pandas as pd

from cbm3_python.cbm3data import cbm3_output_files
from cbm3_python.cbm3data import cbm3_output_descriptions
from cbm3_python.cbm3data import cbm3_output_classifiers
from cbm3_python.cbm3data.cbm3_output_descriptions import ResultsDescriber


def _get_local_file(filename):
    return os.path.join(
        os.path.dirname(os.path.realpath(__file__)), filename)


def _process_pool_indicator_table(loaded_csets, pool_indicators, index_offset):
    pool_indicators_new = \
        cbm3_output_classifiers.replace_with_classifier_set_id(
            pool_indicators, loaded_csets)
    mapping_data = pd.read_csv(
        _get_local_file("pool_indicators_column_mapping.csv"))
    column_map = {
        row.CBM3_raw_column_name: row.RRDB_column_name
        for _, row in mapping_data.dropna().iterrows()}
    pool_indicators_new.rename(columns=column_map, inplace=True)
    pool_indicators_new.drop(columns="RunID", inplace=True)
    pool_indicators_new.insert(
        loc=0, column="PoolIndID",
        value=pool_indicators_new.index + index_offset + 1)
    return pool_indicators_new


def _process_flux_indicator_table(loaded_csets, flux_indicators, index_offset):
    flux_indicators_new = \
        cbm3_output_classifiers.replace_with_classifier_set_id(
            flux_indicators, loaded_csets)
    mapping_data = pd.read_csv(
        _get_local_file("flux_indicators_column_mapping.csv"))
    column_map = {
        row.CBM3_raw_column_name: row.RRDB_column_name
        for _, row in mapping_data.dropna().iterrows()}
    flux_indicators_new.rename(columns=column_map, inplace=True)
    flux_indicators_new.drop(columns="RunID", inplace=True)
    flux_indicators_new.insert(
        loc=0, column="FluxIndicatorID",
        value=flux_indicators_new.index + index_offset + 1)
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
    return flux_indicators_new


def _process_age_indicator_table(loaded_csets, age_indicators, index_offset):
    age_indicators_new = \
        cbm3_output_classifiers.replace_with_classifier_set_id(
            age_indicators, loaded_csets)
    mapping_data = pd.read_csv(
        _get_local_file("age_indicators_column_mapping.csv"))
    column_map = {
        row.CBM3_raw_column_name: row.RRDB_column_name
        for _, row in mapping_data.dropna().iterrows()}
    age_indicators_new.rename(columns=column_map, inplace=True)
    age_indicators_new.drop(columns="RunID", inplace=True)
    age_indicators_new.insert(
        loc=0, column="AgeIndID",
        value=age_indicators_new.index + index_offset + 1)
    return age_indicators_new


def _process_dist_indicator_table(loaded_csets, dist_indicators, index_offset):
    dist_indicators_new = \
        cbm3_output_classifiers.replace_with_classifier_set_id(
            dist_indicators, loaded_csets)
    mapping_data = pd.read_csv(
        _get_local_file("dist_indicators_column_mapping.csv"))
    column_map = {
        row.CBM3_raw_column_name: row.RRDB_column_name
        for _, row in mapping_data.dropna().iterrows()}
    dist_indicators_new.rename(columns=column_map, inplace=True)
    dist_indicators_new.drop(columns="RunID", inplace=True)
    dist_indicators_new.insert(
        loc=0, column="DistIndID",
        value=dist_indicators_new.index + index_offset + 1)
    return dist_indicators_new


def _load_local_column_map(filename):
    mapping_data = pd.read_csv(
        _get_local_file(filename))
    return {
        row.CBM3_raw_column_name: row.RRDB_column_name
        for _, row in mapping_data.dropna().iterrows()}


def _get_replace_with_classifier_set_id_func(loaded_csets):
    def func(df):
        return cbm3_output_classifiers.replace_with_classifier_set_id(
            df, loaded_csets)
    return func


def _get_drop_column_func(column_name):
    def func(df):
        df.drop(columns=column_name, inplace=True)
        return df
    return func


def _get_column_rename_func(column_name_map):
    def func(df):
        df.rename(columns=column_name_map, inplace=True)
        return df
    return func


def _get_add_id_column_func(id_column_name, index_offset):
    def func(df):
        df.insert(
            loc=0, column=id_column_name,
            value=df.index + index_offset + 1)
    return func


def _get_load_functions():


def load_output_relational_tables(cbm_run_results_dir, project_db_path,
                                  aidb_path, out_func, chunksize=None):

    project_data = cbm3_output_descriptions.load_project_level_data(
        project_db_path)

    aidb_data = cbm3_output_descriptions.load_archive_index_data(aidb_path)

    loaded_csets = cbm3_output_classifiers.create_loaded_classifiers(
        project_data.tblClassifiers,
        project_data.tblClassifierSetValues,
        cbm_run_results_dir, chunksize=chunksize)
    project_data.tblClassifierSetValues = \
        cbm3_output_classifiers.melt_loaded_csets(loaded_csets)

    for k, v in aidb_data.__dict__.items():
        out_func(k, v)
    for k, v in project_data.__dict__.items():
        out_func(k, v)

    results_list = [
        {"table_name": "tblAgeIndicators",
         "load_function": cbm3_output_files.load_age_indicators,
         "process_functions": [
             _get_replace_with_classifier_set_id_func(loaded_csets),
             _get_drop_column_func("RunID"),
             _get_column_rename_func(
                 _load_local_column_map("dist_indicators_column_mapping.csv")),
             _get_add_id_column_func("AgeIndID", )
         ]},
        {"table_name": "tblDistIndicators",
         "load_function": cbm3_output_files.load_dist_indicators,
         "process_functions": _process_dist_indicator_table},
        {"table_name": "tblPoolIndicators",
         "load_function": cbm3_output_files.load_pool_indicators,
         "process_functions": _process_pool_indicator_table},
        {"table_name": "tblFluxIndicators",
         "load_function": cbm3_output_files.load_flux_indicators,
         "process_functions": _process_flux_indicator_table}]

    for result_item in results_list:
        result_chunk_iterable = cbm3_output_files.make_iterable(
            result_item["load_function"], cbm_run_results_dir, chunksize)
        index_offset = 0
        for chunk in result_chunk_iterable:
            processed_chunk = result_item["process_function"](
                loaded_csets, chunk, index_offset)
            index_offset = index_offset + len(processed_chunk.index)
            out_func(
                result_item["table_name"],
                processed_chunk)


def load_output_descriptive_tables(cbm_run_results_dir, project_db_path,
                                   aidb_path, out_func,
                                   classifier_value_field="Name",
                                   chunksize=None):
    project_data = cbm3_output_descriptions.load_project_level_data(
        project_db_path)
    loaded_csets = cbm3_output_classifiers.create_loaded_classifiers(
        project_data.tblClassifiers,
        project_data.tblClassifierSetValues,
        cbm_run_results_dir, chunksize=chunksize)
    d = ResultsDescriber(
        project_db_path, aidb_path, loaded_csets, classifier_value_field)
    results_list = [
        {"table_name": "age_indicators",
         "load_function": cbm3_output_files.load_age_indicators,
         "process_function": _process_age_indicator_table,
         "describe_function": d.merge_age_indicator_descriptions},
        {"table_name": "dist_indicators",
         "load_function": cbm3_output_files.load_dist_indicators,
         "process_function": _process_dist_indicator_table,
         "describe_function": d.merge_dist_indicator_descriptions},
        {"table_name": "pool_indicators",
         "load_function": cbm3_output_files.load_pool_indicators,
         "process_function": _process_pool_indicator_table,
         "describe_function": d.merge_pool_indicator_descriptions},
        {"table_name": "flux_indicators",
         "load_function": cbm3_output_files.load_flux_indicators,
         "process_function": _process_flux_indicator_table,
         "describe_function": d.merge_flux_indicator_descriptions}]

    for result_item in results_list:
        result_chunk_iterable = cbm3_output_files.make_iterable(
            result_item["load_function"], cbm_run_results_dir, chunksize)
        index_offset = 0
        for chunk in result_chunk_iterable:
            processed_chunk = result_item["process_function"](
                loaded_csets, chunk, index_offset)
            index_offset = index_offset + len(processed_chunk.index)
            described_chunk = result_item["describe_function"](processed_chunk)
            out_func(result_item["table_name"], described_chunk)
