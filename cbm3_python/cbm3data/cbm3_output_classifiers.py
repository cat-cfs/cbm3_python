import pandas as pd
from cbm3_python.cbm3data import cbm3_output_files
from warnings import warn


def create_loaded_classifiers(tblClassifiers, tblClassifierSetValues,
                              cbm_results_dir, chunksize=None):

    raw_cset_columns = [
        f"c{x+1}" for x in range(0, len(tblClassifiers.index))]
    cset_pivot = tblClassifierSetValues.pivot(
        index="ClassifierSetID", columns="ClassifierID")
    cset_pivot = cset_pivot.rename_axis(None)
    cset_pivot.columns = raw_cset_columns
    cset_pivot = cset_pivot.reset_index()
    cset_pivot = cset_pivot.rename(columns={"index": "ClassifierSetID"})
    if cset_pivot.isna().any().any():
        nan_csets = cset_pivot[cset_pivot.isna().any(axis=1)]
        num_nans = len(nan_csets.index)
        error = "ClassifierSetID with missing ClassifierValueID " \
                f"values: {list(nan_csets.head(5).ClassifierSetID)}"
        if num_nans > 5:
            error = error + f" ... {num_nans - 5} More"
        warn(error)
        cset_pivot = cset_pivot.dropna().astype("int64")

    raw_classifier_data = pd.DataFrame()

    pool_indicators = cbm3_output_files.load_pool_indicators(
        cbm_results_dir, chunksize)
    if not chunksize:
        pool_indicators = [pool_indicators]
    for chunk in pool_indicators:
        for col in raw_cset_columns:
            chunk.loc[chunk[col] <= 0, col] = 1
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
    cset_pivot_out = cset_pivot.drop_duplicates(
        subset=raw_cset_columns, ignore_index=True)
    for col in raw_cset_columns:
        raw_table.loc[raw_table[col] <= 0, col] = 1
    output = cset_pivot_out.merge(
        raw_table, left_on=raw_cset_columns,
        right_on=raw_cset_columns, copy=True, validate="1:m")
    classifier_set_col = output["ClassifierSetID"]
    output = output.drop(columns=[f"c{x}" for x in range(1, 11)])
    output = output[list(output.columns)[1:]]
    output.insert(
        loc=classifier_set_insertion_index, column="UserDefdClassSetID",
        value=classifier_set_col)
    return output


def melt_loaded_csets(csets):

    csets_melt = csets.copy()
    csets_melt.columns = \
        [csets_melt.columns[0]] + list(range(1, len(csets_melt.columns)))
    csets_melt = pd.melt(csets_melt, id_vars=["ClassifierSetID"])
    csets_melt = csets_melt.rename(columns={
        "variable": "ClassifierID",
        "value": "ClassifierValueID"
    }).sort_values(by=["ClassifierSetID", "ClassifierID"])

    csets_melt.loc[
        pd.isna(csets_melt.ClassifierValueID), "ClassifierValueID"] = 1
    csets_melt = csets_melt.astype("int64")
    return csets_melt


def create_classifier_sets(loaded_csets, tblClassifiers, tblClassifierValues,
                           tblClassifierAggregates):

    mapped_output_series = []
    sorted_classifiers = enumerate(
        tblClassifiers.sort_values(by="ClassifierID").ClassifierID)
    for classifier_index, classifier_id in sorted_classifiers:
        classifier_value_map = {
            row.ClassifierValueID: row.Name
            for _, row in tblClassifierValues[
                tblClassifierValues.ClassifierID == classifier_id].iterrows()}
        classifier_aggregate_map = {
            row.AggregateID: row.Name
            for _, row in tblClassifierAggregates[
                tblClassifierAggregates.ClassifierID == classifier_id
                ].iterrows()}

        classifier_value_map.update(classifier_aggregate_map)
        output_series = loaded_csets[loaded_csets.columns[classifier_index+1]]
        output_series = output_series.map(classifier_value_map)
        if pd.isna(output_series).any():
            raise ValueError("unmapped classifier values detected")
        mapped_output_series.append(output_series)

    tblClassifierSetName = pd.DataFrame(
        data={
            f"c{i_out_series}": out_series
            for i_out_series, out_series in
            enumerate(mapped_output_series)}
        ).apply(lambda x: ",".join(x), axis=1)
    tblClassifierSets = pd.DataFrame(
        data={
            "ClassifierSetID": loaded_csets.ClassifierSetID,
            "Name": tblClassifierSetName})
    return tblClassifierSets
