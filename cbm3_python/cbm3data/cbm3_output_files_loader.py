import os
import pandas as pd
import functools
from cbm3_python.cbm3data import cbm3_output_files
from cbm3_python.cbm3data import disturbance_reconciliation
from cbm3_python.cbm3data import cbm3_output_descriptions
from cbm3_python.cbm3data import cbm3_output_classifiers
from cbm3_python.cbm3data.cbm3_output_descriptions import ResultsDescriber


class LoadFunctionFactory:
    def __init__(
        self,
        loaded_csets,
        describer,
        cbm_project_db_path,
        cbm_output_dir,
        cbm_input_dir,
        chunksize,
    ):
        self.describer = describer
        self.cbm_project_db_path = cbm_project_db_path
        self.loaded_csets = loaded_csets
        self.cbm_output_dir = cbm_output_dir
        self.cbm_input_dir = cbm_input_dir
        self.chunksize = chunksize

    def _wrap_unchunkable(self, func, *args, **kwargs):
        def f():
            return [func(*args, **kwargs)]

        return f

    def _wrap_chunkable(self, func, *args, **kwargs):
        def f():
            result = func(*args, **kwargs)
            if self.chunksize:
                return result
            else:
                return [result]

        return f

    def _wrap_load_func(self, func):
        return self._wrap_chunkable(func, self.cbm_output_dir, self.chunksize)

    def get_all(self):
        """Get the functions to load, process and describe CBM3 output
        datasets.

        Returns:
            dict: a dictionary containing the load functions (values) for
                each table name (keys)
        """
        return {
            "tblAgeIndicators": {
                "load_function": self._wrap_load_func(
                    cbm3_output_files.load_age_indicators
                ),
                "process_function": lambda index_offset: _compose(
                    _get_replace_with_classifier_set_id_func(
                        self.loaded_csets
                    ),
                    _get_drop_column_func("RunID"),
                    _get_column_rename_func(
                        _load_local_column_map(
                            "age_indicators_column_mapping.csv"
                        )
                    ),
                    _get_add_id_column_func("AgeIndID", index_offset),
                ),
                "describe_function": _compose(
                    self.describer.merge_landclass_description,
                    self.describer.merge_spatial_unit_description,
                    self.describer.merge_age_class_descriptions,
                    self.describer.merge_classifier_set_description,
                )
                if self.describer
                else None,
            },
            "tblDistIndicators": {
                "load_function": self._wrap_load_func(
                    cbm3_output_files.load_dist_indicators
                ),
                "process_function": lambda index_offset: _compose(
                    _get_replace_with_classifier_set_id_func(
                        self.loaded_csets
                    ),
                    _get_drop_column_func("RunID"),
                    _get_column_rename_func(
                        _load_local_column_map(
                            "dist_indicators_column_mapping.csv"
                        )
                    ),
                    _get_add_id_column_func("DistIndID", index_offset),
                ),
                "describe_function": _compose(
                    self.describer.merge_landclass_description,
                    self.describer.merge_spatial_unit_description,
                    self.describer.merge_disturbance_type_description,
                    self.describer.merge_classifier_set_description,
                )
                if self.describer
                else None,
            },
            "tblPoolIndicators": {
                "load_function": self._wrap_load_func(
                    cbm3_output_files.load_pool_indicators
                ),
                "process_function": lambda index_offset: _compose(
                    _get_replace_with_classifier_set_id_func(
                        self.loaded_csets
                    ),
                    _get_drop_column_func("RunID"),
                    _get_column_rename_func(
                        _load_local_column_map(
                            "pool_indicators_column_mapping.csv"
                        )
                    ),
                    _get_add_id_column_func("PoolIndID", index_offset),
                ),
                "describe_function": _compose(
                    self.describer.merge_landclass_description,
                    self.describer.merge_spatial_unit_description,
                    self.describer.merge_classifier_set_description,
                )
                if self.describer
                else None,
            },
            "tblFluxIndicators": {
                "load_function": self._wrap_load_func(
                    cbm3_output_files.load_flux_indicators
                ),
                "process_function": lambda index_offset: _compose(
                    _get_replace_with_classifier_set_id_func(
                        self.loaded_csets
                    ),
                    _get_drop_column_func("RunID"),
                    _get_column_rename_func(
                        _load_local_column_map(
                            "flux_indicators_column_mapping.csv"
                        )
                    ),
                    _get_add_id_column_func("FluxIndicatorID", index_offset),
                    _get_gross_growth_column_funcs(),
                ),
                "describe_function": _compose(
                    self.describer.merge_landclass_description,
                    self.describer.merge_spatial_unit_description,
                    self.describer.merge_disturbance_type_description,
                    self.describer.merge_classifier_set_description,
                )
                if self.describer
                else None,
            },
            "tblNIRSpecialOutput": {
                "load_function": self._wrap_load_func(
                    cbm3_output_files.load_nir_output
                ),
                "process_function": lambda index_offset: _compose(
                    _get_add_id_column_func("usLessPkField", index_offset)
                ),
                "describe_function": _compose(
                    self.describer.merge_spatial_unit_description,
                    self.describer.merge_disturbance_type_description,
                )
                if self.describer
                else None,
            },
            "tblDistNotRealized": {
                "load_function": self._wrap_load_func(
                    cbm3_output_files.load_nodist
                ),
                "process_function": lambda index_offset: _compose(
                    _get_drop_column_func("RunID")
                ),
                "describe_function": _compose(
                    self.describer.merge_disturbance_type_description
                )
                if self.describer
                else None,
            },
            "tblSVL": {
                "load_function": self._wrap_chunkable(
                    cbm3_output_files.load_svl_files,
                    self.cbm_input_dir,
                    self.cbm_output_dir,
                    self.chunksize,
                ),
                "process_function": lambda index_offset: _compose(
                    _get_add_id_column_func("SVLID", index_offset),
                    _get_column_rename_func(
                        {
                            "LastDisturbanceTypeID": "DistTypeID",
                            "landclass": "LandClassID",
                        }
                    ),
                    _get_replace_with_classifier_set_id_func(
                        self.loaded_csets
                    ),
                ),
                "describe_function": _compose(
                    self.describer.merge_disturbance_type_description
                )
                if self.describer
                else None,
            },
            "tblDisturbanceSeries": {
                "load_function": self._wrap_load_func(
                    cbm3_output_files.load_distseries
                ),
                "process_function": lambda index_offset: _compose(
                    _get_column_rename_func({"timestep": "TimeStep"}),
                ),
                "describe_function": lambda df: df if self.describer else None,
            },
            "tblAccountingRuleDiagnostics": {
                "load_function": self._wrap_load_func(
                    cbm3_output_files.load_accdiagnostics
                ),
                "process_function": lambda index_offset: lambda df: df,
                "describe_function": lambda df: df if self.describer else None,
            },
            "tblPreDisturbanceAge": {
                "load_function": self._wrap_load_func(
                    cbm3_output_files.load_predistage
                ),
                "process_function": lambda index_offset: _compose(
                    # drop an empty column
                    lambda df: df.drop(df.columns[13], axis=1),
                    _get_add_id_column_func("PreDistAgeID", index_offset),
                    _get_column_rename_func(
                        _update_dict(
                            {
                                "spuid": "SPUID",
                                "dist_type": "DistTypeID",
                                "timestep": "TimeStep",
                                "k0": "LandClassID",
                            },
                            {f"k{x}": f"kf{x+1}" for x in range(1, 6)},
                            {f"c{x}": f"c{x+1}" for x in range(0, 10)},
                        )
                    ),
                    _get_replace_with_classifier_set_id_func(
                        self.loaded_csets
                    ),
                ),
                "describe_function": _compose(
                    self.describer.merge_landclass_description,
                    self.describer.merge_spatial_unit_description,
                    self.describer.merge_disturbance_type_description,
                    self.describer.merge_classifier_set_description,
                )
                if self.describer
                else None,
            },
            "tblDisturbanceReconciliation": {
                "load_function": self._wrap_unchunkable(
                    disturbance_reconciliation.create,
                    self.cbm_project_db_path,
                    self.cbm_input_dir,
                    self.cbm_output_dir,
                ),
                "process_function": lambda index_offset: lambda df: df,
                "describe_function": lambda df: df if self.describer else None,
            },
            "tblRandomSeed": {
                "load_function": self._wrap_load_func(
                    cbm3_output_files.load_seed
                ),
                "process_function": lambda index_offset: _compose(
                    _get_drop_column_func(["MonteCarloAssumptionID", "RunID"])
                ),
                "describe_function": lambda df: df,
            },
            "tblPoolsSpatial": {
                "load_function": self._wrap_load_func(
                    cbm3_output_files.load_spatial_pools
                ),
                "process_function": lambda index_offset: _compose(
                    _get_replace_with_classifier_set_id_func(
                        self.loaded_csets
                    ),
                    _get_drop_column_func("RunID"),
                    _get_column_rename_func(
                        _update_dict(
                            _load_local_column_map(
                                "pool_indicators_column_mapping.csv"
                            ),
                            {"SVOID": "SVOID", "Age": "Age"},
                        )
                    ),
                ),
                "describe_function": _compose(
                    self.describer.merge_landclass_description,
                    self.describer.merge_spatial_unit_description,
                    self.describer.merge_classifier_set_description,
                )
                if self.describer
                else None,
            },
            "tblFluxSpatial": {
                "load_function": self._wrap_load_func(
                    cbm3_output_files.load_spatial_flux
                ),
                "process_function": lambda index_offset: _compose(
                    _get_replace_with_classifier_set_id_func(
                        self.loaded_csets
                    ),
                    _get_drop_column_func("RunID"),
                    _get_column_rename_func(
                        _update_dict(
                            _load_local_column_map(
                                "flux_indicators_column_mapping.csv"
                            ),
                            {"SVOID": "SVOID"},
                        )
                    ),
                ),
                "describe_function": _compose(
                    self.describer.merge_landclass_description,
                    self.describer.merge_spatial_unit_description,
                    self.describer.merge_disturbance_type_description,
                    self.describer.merge_classifier_set_description,
                )
                if self.describer
                else None,
            },
        }


def _update_dict(d1, *d):
    for _d in d:
        d1.update(_d)
    return d1


def _get_local_file(filename):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), filename)


def _load_local_column_map(filename):
    mapping_data = pd.read_csv(_get_local_file(filename))
    return {
        row.CBM3_raw_column_name: row.RRDB_column_name
        for _, row in mapping_data.dropna().iterrows()
    }


def _get_replace_with_classifier_set_id_func(loaded_csets):
    def func(df):
        return cbm3_output_classifiers.replace_with_classifier_set_id(
            df, loaded_csets
        )

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
            loc=0, column=id_column_name, value=df.index + index_offset + 1
        )
        return df

    return func


def _get_gross_growth_column_funcs():
    def func(df):
        # gross growth AG and BG are composite flux indicators that are not
        # included in RAW CBM3 output, but are present in tblFluxIndicators
        df["GrossGrowth_AG"] = df[
            [
                "DeltaBiomass_AG",
                "MerchLitterInput",
                "FolLitterInput",
                "OthLitterInput",
                "SubMerchLitterInput",
            ]
        ].sum(axis=1)
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

    for specified functions f1(x), f2(x), ..., fn(x)

    return a composed function fn( ... f2(f1(x)))
    """

    def compose2(f, g):
        return lambda *a, **kw: f(g(*a, **kw))

    fs = list(fs)
    fs.reverse()
    return functools.reduce(compose2, fs)


def _get_cbm_input_dir(cbm_output_dir):
    return os.path.realpath(os.path.join(cbm_output_dir, "..", "input"))


def load_output_relational_tables(
    cbm_output_dir,
    project_db_path,
    aidb_path,
    out_func,
    chunksize=None,
    include_spatial=False,
    include_diagnostics=False,
):
    """Load all CBM datasets to a relational database output

    Args:
        cbm_output_dir (str): path to a CBMRun/output dir
        project_db_path (str): path to a CBM-CFS3 access database project
        aidb_path (str): path to a CBM-CFS3 archive index database
        out_func (func): a function of (table_name, data) called for each
            loaded data chunk.  This function can be called more than one
            time per table_name if the load is split into multiple chunks.
        chunksize (int, optional): If specified sets a maximum number of rows
            to hold in memory at a given time while loading output.
            Defaults to None.
        include_spatial (bool, optional): If set to true "tblPoolsSpatial",
            and "tblFluxSpatial" will be loaded if present, and they will
            otherwise be ignored. Defaults to False.
        include_diagnostics (bool, optional): If set to true extra diagnostic
            tables will be loaded. Defaults to False.
    """
    project_data = cbm3_output_descriptions.load_project_level_data(
        project_db_path
    )

    aidb_data = cbm3_output_descriptions.load_archive_index_data(aidb_path)

    loaded_csets = cbm3_output_classifiers.create_loaded_classifiers(
        project_data.tblClassifiers,
        project_data.tblClassifierSetValues,
        cbm_output_dir,
        chunksize=chunksize,
    )
    project_data.tblClassifierSetValues = (
        cbm3_output_classifiers.melt_loaded_csets(loaded_csets)
    )
    project_data.tblClassifierSets = (
        cbm3_output_classifiers.create_classifier_sets(
            loaded_csets,
            project_data.tblClassifiers,
            project_data.tblClassifierValues,
            project_data.tblClassifierAggregates,
        )
    )
    out_project_data = (
        cbm3_output_descriptions.create_project_level_output_tables(
            project_data
        )
    )

    for k, v in aidb_data.__dict__.items():
        out_func(k, v)
    for k, v in out_project_data.__dict__.items():
        out_func(k, v)
    out_func("tblAgeClasses", cbm3_output_descriptions.load_age_classes())
    load_func_factory = LoadFunctionFactory(
        loaded_csets,
        describer=None,
        cbm_project_db_path=project_db_path,
        cbm_output_dir=cbm_output_dir,
        cbm_input_dir=_get_cbm_input_dir(cbm_output_dir),
        chunksize=chunksize,
    )
    load_funcs = load_func_factory.get_all()
    for table_name in load_funcs.keys():
        load_functions = load_funcs[table_name]
        if (
            table_name in ["tblPoolsSpatial", "tblFluxSpatial"]
            and not include_spatial
        ):
            continue
        if (
            table_name
            in ["tblAccountingRuleDiagnostics", "tblDisturbanceSeries"]
            and not include_diagnostics
        ):
            continue
        result_chunk_iterable = load_functions["load_function"]()
        index_offset = 0
        for chunk in result_chunk_iterable:
            process_function = load_functions["process_function"](index_offset)
            processed_chunk = process_function(chunk)
            index_offset = index_offset + len(processed_chunk.index)
            out_func(table_name, processed_chunk)


def load_output_descriptive_tables(
    cbm_output_dir, project_db_path, aidb_path, out_func, chunksize=None
):
    """Load all CBM datasets to a descriptive format.

    Args:

        cbm_output_dir (str): path to a CBMRun/output dir
        project_db_path (str): path to a CBM-CFS3 access database project
        aidb_path (str): path to a CBM-CFS3 archive index database
        out_func (func): a function of (table_name, data) called for each
            loaded data chunk.  This function can be called more than one
            time per table_name if the load is split into multiple chunks.
        chunksize (int, optional): If specified sets a maximum number of rows
            to hold in memory at a given time while loading output.
            Defaults to None.

    """
    project_data = cbm3_output_descriptions.load_project_level_data(
        project_db_path
    )
    loaded_csets = cbm3_output_classifiers.create_loaded_classifiers(
        project_data.tblClassifiers,
        project_data.tblClassifierSetValues,
        cbm_output_dir,
        chunksize=chunksize,
    )
    describer = ResultsDescriber(
        project_db_path, aidb_path, loaded_csets, classifier_value_field="Name"
    )
    load_func_factory = LoadFunctionFactory(
        loaded_csets,
        describer=describer,
        cbm_project_db_path=project_db_path,
        cbm_output_dir=cbm_output_dir,
        cbm_input_dir=_get_cbm_input_dir(cbm_output_dir),
        chunksize=chunksize,
    )
    load_funcs = load_func_factory.get_all()
    for table_name in load_funcs.keys():
        load_functions = load_funcs[table_name]

        result_chunk_iterable = load_functions["load_function"]()
        index_offset = 0
        for chunk in result_chunk_iterable:
            process_function = load_functions["process_function"](index_offset)
            processed_chunk = process_function(chunk)
            index_offset = index_offset + len(processed_chunk.index)
            describe_func = load_functions["describe_function"]
            described_chunk = describe_func(processed_chunk)
            out_func(table_name, described_chunk)
