import os
import pandas as pd
import functools
from cbm3_python.cbm3data import cbm3_output_files
from cbm3_python.cbm3data import cbm3_output_descriptions
from cbm3_python.cbm3data import cbm3_output_classifiers
from cbm3_python.cbm3data.cbm3_output_descriptions import ResultsDescriber


def _update_dict(d1, d2):
    d1.update(d2)
    return d1


LOAD_FUNCTIONS = {
    "tblAgeIndicators": {
        "load_function": cbm3_output_files.load_age_indicators,
        "process_function": lambda loaded_csets, index_offset: _compose(
            _get_replace_with_classifier_set_id_func(loaded_csets),
            _get_drop_column_func("RunID"),
            _get_column_rename_func(
                _load_local_column_map("age_indicators_column_mapping.csv")),
            _get_add_id_column_func("AgeIndID", index_offset)),
        "describe_function": lambda describer: _compose(
            describer.merge_landclass_description,
            describer.merge_spatial_unit_description,
            describer.merge_classifier_set_description)},
    "tblDistIndicators": {
        "load_function": cbm3_output_files.load_dist_indicators,
        "process_function": lambda loaded_csets, index_offset: _compose(
            _get_replace_with_classifier_set_id_func(loaded_csets),
            _get_drop_column_func("RunID"),
            _get_column_rename_func(
                _load_local_column_map("dist_indicators_column_mapping.csv")),
            _get_add_id_column_func("DistIndID", index_offset)),
        "describe_function": lambda describer: _compose(
            describer.merge_landclass_description,
            describer.merge_spatial_unit_description,
            describer.merge_disturbance_type_description,
            describer.merge_classifier_set_description)},
    "tblPoolIndicators": {
        "load_function": cbm3_output_files.load_pool_indicators,
        "process_function": lambda loaded_csets, index_offset: _compose(
            _get_replace_with_classifier_set_id_func(loaded_csets),
            _get_drop_column_func("RunID"),
            _get_column_rename_func(
                _load_local_column_map("pool_indicators_column_mapping.csv")),
            _get_add_id_column_func("PoolIndID", index_offset)),
        "describe_function": lambda describer: _compose(
            describer.merge_landclass_description,
            describer.merge_spatial_unit_description,
            describer.merge_classifier_set_description)},
    "tblFluxIndicators": {
        "load_function": cbm3_output_files.load_flux_indicators,
        "process_function": lambda loaded_csets, index_offset: _compose(
            _get_replace_with_classifier_set_id_func(loaded_csets),
            _get_drop_column_func("RunID"),
            _get_column_rename_func(
                _load_local_column_map("flux_indicators_column_mapping.csv")),
            _get_add_id_column_func("FluxIndicatorID", index_offset),
            _get_gross_growth_column_funcs()),
        "describe_function": lambda describer: _compose(
            describer.merge_landclass_description,
            describer.merge_spatial_unit_description,
            describer.merge_disturbance_type_description,
            describer.merge_classifier_set_description)},
    "tblNIRSpecialOutput": {
        "load_function": cbm3_output_files.load_nir_output,
        "process_function": lambda loaded_csets, index_offset: lambda df: df,
        "describe_function": lambda describer: _compose(
            describer.merge_spatial_unit_description,
            describer.merge_disturbance_type_description)
    },
    "tblDistNotRealized": {
        "load_function": cbm3_output_files.load_nodist,
        "process_function": lambda loaded_csets, index_offset: _compose(
            _get_drop_column_func("RunID")
        ),
        "describe_function": lambda describer: _compose(
            describer.merge_disturbance_type_description)
    },
    "tblSVL": {
        "load_function": cbm3_output_files.load_svl_files,
        "process_function": lambda loaded_csets, index_offset: _compose(
            _get_column_rename_func({"LastDisturbanceTypeID": "DistTypeID"}),
            _get_replace_with_classifier_set_id_func(loaded_csets)
        ),
        "describe_function": lambda describer: _compose(
            describer.merge_disturbance_type_description
        )
    },
    "tblDisturbanceSeries": {
        "load_function": cbm3_output_files.load_distseries,
        "process_function": lambda loaded_csets, index_offset: _compose(
            _get_column_rename_func({"timestep": "TimeStep"}),
        ),
        "describe_function": lambda describer: lambda df: df
    },
    "tblAccountingRuleDiagnostics": {
        "load_function": cbm3_output_files.load_accdiagnostics,
        "process_function": lambda loaded_csets, index_offset: lambda df: df,
        "describe_function": lambda describer: lambda df: df
    },
    "tblPreDisturbanceAge": {
        "load_function": cbm3_output_files.load_svl_files,
        "process_function": lambda loaded_csets, index_offset: _compose(
            _get_replace_with_classifier_set_id_func(loaded_csets),
            _get_column_rename_func({
                "spuid": "SPUID", "dist_type": "DistTypeID",
                "timestep": "TimeStep", "k0": "LandClassID",
                "k1": "kf2", "k2": "kf3", "k3": "kf4", "k4": "kf5",
                "k5": "kf6"}),
        ),
        "describe_function": lambda describer: _compose(
            describer.merge_landclass_description,
            describer.merge_spatial_unit_description,
            describer.merge_disturbance_type_description,
            describer.merge_classifier_set_description)
    },
    "tblDisturbanceReconciliation": {
        "load_function": cbm3_output_files.load_disturbance_reconciliation,
        # TODO: need to review the values in this output and also figure out
        # disturbance groups
        "process_function": lambda loaded_csets, index_offset: lambda df: df,
        "describe_function": lambda describer: lambda df: df,
    },
    "tblPoolsSpatial": {
        "load_function": cbm3_output_files.load_spatial_pools,
        "process_function": lambda loaded_csets, index_offset: _compose(
            _get_replace_with_classifier_set_id_func(loaded_csets),
            _get_drop_column_func("RunID"),
            _get_column_rename_func(
                _update_dict(
                    _load_local_column_map(
                        "pool_indicators_column_mapping.csv"),
                    {"SVOID": "SVOID", "Age": "Age"}))
        ),
        "describe_function": lambda describer: _compose(
            describer.merge_landclass_description,
            describer.merge_spatial_unit_description,
            describer.merge_classifier_set_description)
    },
    "tblFluxSpatial": {
        "load_function": cbm3_output_files.load_flux_indicators,
        "process_function": lambda loaded_csets, index_offset: _compose(
            _get_replace_with_classifier_set_id_func(loaded_csets),
            _get_drop_column_func("RunID"),
            _get_column_rename_func(
                _update_dict(
                    _load_local_column_map(
                        "flux_indicators_column_mapping.csv"),
                    {"SVOID": "SVOID"}
                )),
        ),
        "describe_function": lambda describer: _compose(
            describer.merge_landclass_description,
            describer.merge_spatial_unit_description,
            describer.merge_disturbance_type_description,
            describer.merge_classifier_set_description)
    }
}


def _get_local_file(filename):
    return os.path.join(
        os.path.dirname(os.path.realpath(__file__)), filename)


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


def _get_gross_growth_column_funcs():
    def func(df):
        # gross growth AG and BG are composite flux indicators that are not
        # included in RAW CBM3 output, but are present in tblFluxIndicators
        df["GrossGrowth_AG"] = df[
            ["DeltaBiomass_AG", "MerchLitterInput", "FolLitterInput",
             "OthLitterInput", "SubMerchLitterInput"]].sum(axis=1)
        df.loc[df.DistTypeID != 0, "GrossGrowth_AG"] = 0.0
        df["GrossGrowth_BG"] = df[
            ["DeltaBiomass_BG", "CoarseLitterInput", "FineLitterInput"]
        ].sum(axis=1)
        df.loc[df.DistTypeID != 0, "GrossGrowth_BG"] = 0.0
        return df
    return func


def _compose(*fs):
    """Adapted from:
    https://stackoverflow.com/questions/16739290/composing-functions-in-python
    """
    def compose2(f, g):
        return lambda *a, **kw: f(g(*a, **kw))

    return functools.reduce(compose2, fs)


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

    results_table_list = [
        "tblAgeIndicators",
        "tblDistIndicators",
        "tblPoolIndicators",
        "tblFluxIndicators"
        ]
    for table_name in results_table_list:
        load_functions = LOAD_FUNCTIONS[table_name]
        result_chunk_iterable = cbm3_output_files.make_iterable(
            load_functions["load_function"], cbm_run_results_dir, chunksize)
        index_offset = 0
        for chunk in result_chunk_iterable:
            process_function = load_functions["process_function"](
                loaded_csets, index_offset)
            processed_chunk = process_function(chunk)
            index_offset = index_offset + len(processed_chunk.index)
            out_func(table_name, processed_chunk)


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
    describer = ResultsDescriber(
        project_db_path, aidb_path, loaded_csets, classifier_value_field)

    results_table_list = [
        "tblAgeIndicators",
        "tblDistIndicators",
        "tblPoolIndicators",
        "tblFluxIndicators"
        ]
    for table_name in results_table_list:
        load_functions = LOAD_FUNCTIONS[table_name]

        result_chunk_iterable = cbm3_output_files.make_iterable(
            load_functions["load_function"], cbm_run_results_dir, chunksize)
        index_offset = 0
        for chunk in result_chunk_iterable:
            process_function = load_functions["process_function"](
                loaded_csets, index_offset)
            processed_chunk = process_function(chunk)
            index_offset = index_offset + len(processed_chunk.index)
            describe_func = load_functions["describe_function"](describer)
            described_chunk = describe_func(processed_chunk)
            out_func(table_name, described_chunk)
