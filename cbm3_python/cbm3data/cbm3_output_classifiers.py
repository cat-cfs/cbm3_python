import pandas as pd
from cbm3_python.cbm3data import cbm3_output_files


def create_loaded_classifiers(tblClassifiers, tblClassifierSetValues,
                              cbm_results_dir, chunksize=None):

    raw_cset_columns = [f"c{x+1}" for x in range(0, len(tblClassifiers.index))]
    cset_pivot = tblClassifierSetValues.pivot(
        index="ClassifierSetID", columns="ClassifierID")
    cset_pivot = cset_pivot.rename_axis(None)
    cset_pivot.columns = raw_cset_columns
    cset_pivot = cset_pivot.reset_index()
    cset_pivot = cset_pivot.rename(columns={"index": "ClassifierSetID"})

    raw_classifier_data = pd.DataFrame()

    pool_indicators = cbm3_output_files.load_pool_indicators(
        cbm_results_dir, chunksize)
    if not chunksize:
        pool_indicators = [pool_indicators]
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


def replace_with_classifier_set_id(raw_table, cset_pivot):
    classifier_set_insertion_index = raw_table.columns.get_loc("c1")
    raw_cset_columns = [f"c{x+1}" for x in range(0, len(cset_pivot.columns)-1)]
    raw_table = cset_pivot.merge(
        raw_table, left_on=raw_cset_columns,
        right_on=raw_cset_columns, copy=True, validate="1:m")
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
