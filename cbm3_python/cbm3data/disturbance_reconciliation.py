# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import os
import re
import csv
import pandas as pd
import numpy as np
from cbm3_python.cbm3data.accessdb import AccessDB
from warnings import warn
from tempfile import TemporaryFile
from cbm3_python.util import loghelper

logger = loghelper.get_logger()


def _read_disturb_list(path):
    column_names = [
        "DistTypeID",
        "DisturbanceGroup",
        "TimeStepStart",
        "TimeStepEnd",
        "Efficiency",
        "SortCondition",
        "DisturbanceTargetFormat",
        "TargetArea",
        "TargetMerchantableCarbon",
        "TargetProportion",
        "TransitionRuleOptionID",
        "MinTotalBioC",
        "MaxTotalBioC",
        "MinMerchantableBioC",
        "MaxMerchantableBioC",
        "MinLastDisturbancetype",
        "MaxLastDisturbancetype",
        "MinTimesincelastdisturbance",
        "MaxTimesincelastdisturbance",
        "MinimumTotalStemSnagC",
        "MaximumTotalStemSnagC",
        "MinimumTotalStemSnagCPlusMerchantableC",
        "MaximumTotalStemSnagCPlusMerchantableC",
        "MinSoftwoodMerchantableCarbon",
        "MaxSoftwoodMerchantableCarbon",
        "MinHardwoodMerchantableCarbon",
        "MaxHardwoodMerchantableCarbon",
        "MinSoftwoodMaturity",
        "MaxSoftwoodMaturity",
        "MinHardwoodMaturity",
        "MaxHardwoodMaturity",
        "MinSoftwoodManagementtype",
        "MaxSoftwoodManagementtype",
        "MinHardwoodManagementType",
        "MaxHardwoodManagementType",
        "MinSoftwoodGrowthCurve",
        "MaxSoftwoodGrowthCurve",
        "MinHardwoodGrowthCurve",
        "MaxHardwoodGrowthCurve",
        "MinSWAge",
        "MaxSWAge",
        "MinHWAge",
        "MaxHWAge",
        "MinSoftwoodStemSnagC",
        "MaxSoftwoodStemSnagC",
        "MinHardwoodStemSnagC",
        "MaxHardwoodStemSnagC",
        "MinSoftwoodMerchantableCPlusStemSnagC",
        "MaxSoftwoodMerchantableCPlusStemSnagC",
        "MinHardwoodMerchantableCPlusStemSnagC",
        "MaxHardwoodMerchantableCPlusStemSnagC",
        "MinClassifier1ID",
        "MaxClassifier1ID",
        "MinClassifier2ID",
        "MaxClassifier2ID",
        "MinClassifier3ID",
        "MaxClassifier3ID",
        "MinClassifier4ID",
        "MaxClassifier4ID",
        "MinClassifier5ID",
        "MaxClassifier5ID",
        "MinClassifier6ID",
        "MaxClassifier6ID",
        "MinClassifier7ID",
        "MaxClassifier7ID",
        "MinClassifier8ID",
        "MaxClassifier8ID",
        "MinClassifier9ID",
        "MaxClassifier9ID",
        "MinClassifier10ID",
        "MaxClassifier10ID",
    ]

    with open(path) as f, TemporaryFile("w+") as t:
        for line in f:
            if len(line.strip().split()) <= 1:
                continue
            t.write(line)
        t.seek(0)

        return pd.read_csv(
            t,
            sep=r'\s+',
            header=None,
            names=column_names,
            quoting=csv.QUOTE_NONE,
        )


def _get_project_events(project_path):
    events_sql = """
        SELECT
            tblDisturbanceEvents.DisturbanceEventID,
            tblDisturbanceEvents.DisturbanceGroupScenarioID,
            tblDisturbanceGroupScenario.SPUGroupID,
            tblDisturbanceGroupScenario.DistTypeID,
            tblDisturbanceType.DefaultDistTypeID,
            tblDisturbanceEvents.TimeStepFinish,
            tblDisturbanceEvents.Efficiency,
            tblDisturbanceEvents.DistArea,
            tblDisturbanceEvents.MerchCarbonToDisturb,
            tblDisturbanceEvents.PropOfRecordToDisturb
        FROM tblDisturbanceType INNER JOIN (
            tblDisturbanceGroupScenario INNER JOIN tblDisturbanceEvents ON
              tblDisturbanceGroupScenario.DisturbanceGroupScenarioID =
                tblDisturbanceEvents.DisturbanceGroupScenarioID) ON
            tblDisturbanceType.DistTypeID =
            tblDisturbanceGroupScenario.DistTypeID
        ORDER BY
          tblDisturbanceEvents.TimeStepStart,
          tblDisturbanceEvents.DisturbanceGroupScenarioID,
          tblDisturbanceEvents.DisturbanceEventID,
          tblDisturbanceEvents.TimeStepFinish;
    """
    with AccessDB(project_path) as accessdb:
        project_events = accessdb.as_data_frame(events_sql)
    return project_events


def _create_merged_disturbance_events(
    project_events, disturb_lst, report_fil_data
):
    # check that Proportion Targets are not used
    if (project_events["PropOfRecordToDisturb"] > 0).any():
        # this function does not support proportion targets
        return None
    project_events["cbm_disturbance_group"] = disturb_lst["DisturbanceGroup"]

    project_events_area_targets = project_events[
        project_events["DistArea"] > 0
    ].sort_values(
        by=[
            "DistArea",
            "TimeStepFinish",
            "DisturbanceGroupScenarioID",
            "DisturbanceEventID",
        ]
    )

    report_fil_data_area_targets = report_fil_data[
        report_fil_data["Target Area"] > 0
    ].sort_values(by="Target Area")
    merged_area_target_events = pd.merge_asof(
        left=project_events_area_targets,
        right=report_fil_data_area_targets,
        left_by=["TimeStepFinish", "DistTypeID", "cbm_disturbance_group"],
        right_by=["Timestep", "Disturbance Type", "Disturbance Group"],
        left_on="DistArea",
        right_on="Target Area",
        direction="nearest",
    )

    project_events_merch_targets = project_events[
        project_events["MerchCarbonToDisturb"] > 0
    ].sort_values(
        by=[
            "MerchCarbonToDisturb",
            "TimeStepFinish",
            "DisturbanceGroupScenarioID",
            "DisturbanceEventID",
        ]
    )

    report_fil_data_merch_targets = report_fil_data[
        report_fil_data["Target Biomass C"] > 0
    ].sort_values(by="Target Biomass C")

    merged_merch_target_events = pd.merge_asof(
        left=project_events_merch_targets,
        right=report_fil_data_merch_targets,
        left_by=["TimeStepFinish", "DistTypeID", "cbm_disturbance_group"],
        right_by=["Timestep", "Disturbance Type", "Disturbance Group"],
        left_on="MerchCarbonToDisturb",
        right_on="Target Biomass C",
        direction="nearest",
    )

    merged_events = pd.concat(
        [merged_area_target_events, merged_merch_target_events]
    ).sort_values(
        by=[
            "TimeStepFinish",
            "SPUGroupID",
            "DisturbanceGroupScenarioID",
            "DisturbanceEventID",
        ]
    )

    # qaqc
    if len(merged_events.index) != len(project_events.index):
        warning_text = (
            "In processing disturbance_reconcilition, the number "
            "of records found in report.fil does not correspond to "
            "the number of records in tblDisturbanceEvents."
        )
        logger.warning(warning_text)
        warn(warning_text)
    merged_dist_area = merged_events["DistArea"].sum()
    merged_merch_c = merged_events["MerchCarbonToDisturb"].sum()
    project_dist_area = project_events["DistArea"].sum()
    project_merch_c = project_events["MerchCarbonToDisturb"].sum()
    if not np.allclose(
        [
            merged_dist_area,
            merged_merch_c,
        ],
        [
            project_dist_area,
            project_merch_c,
        ],
    ):
        warning_text = (
            "In processing disturbance_reconcilition table, the areas "
            "or merch C targets derived from the report.fil log did not "
            "match closely with those in the project database: Sum of "
            f"project area targets {project_dist_area}, Sum of report.fil "
            f"derived area targets {merged_dist_area}, Sum of project "
            f"merch C targets {project_merch_c}, sum of report.fil "
            f"derived merch C targets {merged_merch_c}"
        )
        logger.warning(warning_text)
        warn(warning_text)

    # select and rename_columns
    return merged_events[
        [
            "DisturbanceEventID",
            "DisturbanceGroupScenarioID",
            "SPUGroupID",
            "DistTypeID",
            "TimeStepFinish",
            "Efficiency_x",
            "DistArea",
            "MerchCarbonToDisturb",
            "PropOfRecordToDisturb",
            "cbm_disturbance_group",
            "Sort Type",
            "Target Type",
            "Target Area",
            "Eligible Area",
            "Surplus Area",
            "Area Prop'n",
            "Records Eligible",
            "Records Changed",
            "Records Sorted",
            "Target Biomass C",
            "Surplus Biomass C",
            "Biomass C Prop'n",
        ]
    ].rename(
        columns={
            "TimeStepFinish": "TimeStep",
            "Efficiency_x": "Efficiency",
            "cbm_disturbance_group": "Simulation_DisturbanceGroup",
            "Sort Type": "Simulation_SortType",
            "Target Type": "Simulation_TargetType",
            "Target Area": "Simulation_TargetArea",
            "Eligible Area": "Simulation_EligibleArea",
            "Surplus Area": "Simulation_SurplusArea",
            "Area Prop'n": "Simulation_AreaProportion",
            "Records Eligible": "Simulation_RecordsEligible",
            "Records Changed": "Simulation_RecordsChanged",
            "Records Sorted": "Simulation_RecordsSorted",
            "Target Biomass C": "Simulation_TargetBiomassC",
            "Surplus Biomass C": "Simulation_SurplusBiomassC",
            "Biomass C Prop'n": "Simulation_BiomassCProportion",
        }
    )


def _parse_report_fil(inputFile):
    fileContents = inputFile.read()
    matches = re.findall(
        "Disturbance Reconciliation.+?Records Changed:[ \t0-9]+",
        fileContents,
        re.DOTALL,
    )
    # the 'Disturbance Reconciliation' part matches the first bit of text in
    # each block '.+?' matches any character in a non greedy fashion
    # 'Records Changed:[ \t0-9]+' matches the end of each block

    # variable to hold the column names and indexes
    columns = []

    # loop 1, create the collection of names in the Disturbance Reconciliation
    # blocks
    for match in matches:
        for innerMatch in re.split("[\r\n]", match):
            if "Reconciliation" in innerMatch:
                continue
            kvp = re.split("    |[:]", innerMatch)
            # remove the empty entries in the split
            kvp = list(filter(None, kvp))
            # remove leading and trailing whitespace
            key = kvp[0].strip()
            if key not in columns:
                columns.append(key)

    table = {x: [] for x in columns}
    # loop 2, write out all of the values with columns enumerated in loop 1
    for match in matches:
        row = {}
        for col in columns:
            row[col] = np.nan
        for innerMatch in re.split("[\r\n]", match):
            if "Reconciliation" in innerMatch:
                continue
            kvp = re.split("    |[:]", innerMatch)
            kvp = list(filter(None, kvp))
            col = kvp[0].strip()
            value = kvp[1].strip()
            try:
                cast_value = int(value)
            except ValueError:
                cast_value = float(value)
            row[col] = cast_value
        for col, value in row.items():
            table[col].append(value)

    return pd.DataFrame(data=table)


def parse_report_file(report_fil_path):
    """
    Returns a pandas dataframe of the "CBM3" disturbance
    reconciliation output in the specified input file

    Args:
        report_fil_path (str): path to a CBM3 "report.fil" cbm
            output file

    Returns:
        pandas.DataFrame: a dataframe with the disturbance information
    """
    with open(report_fil_path, "r") as fp:
        df = _parse_report_fil(fp)

    # since the columns can vary from run to run, set them consistently here
    columns = [
        "Timestep",
        "Year",
        "Disturbance Type",
        "Default Disturbance Type",
        "Disturbance Group",
        "Sort Type",
        "Target Type",
        "Target Area",
        "Eligible Area",
        "Efficiency",
        "Surplus Area",
        "Area Prop'n",
        "Records Eligible",
        "Records Changed",
        "Records Sorted",
        "Target Biomass C",
        "Surplus Biomass C",
        "Biomass C Prop'n",
    ]

    df = pd.DataFrame(
        index=df.index,
        data={
            col: df[col] if col in df.columns else np.nan for col in columns
        },
    )

    # convert integer values
    integer_cols = ["Records Changed", "Records Eligible", "Records Sorted"]
    for integer_col in integer_cols:
        df[integer_col] = df[integer_col].fillna(0).astype("int64")

    # covert columns which are sometimes incorrectly inferred to be integers
    # (if no floating point data is in the column)
    float_cols = [
        "Target Area",
        "Eligible Area",
        "Efficiency",
        "Surplus Area",
        "Area Prop'n",
        "Target Biomass C",
        "Surplus Biomass C",
        "Biomass C Prop'n",
    ]
    for float_col in float_cols:
        df[float_col] = df[float_col].astype("float64")

    return df


def create(project_path, cbm_input_dir, cbm_output_dir):
    return _create_merged_disturbance_events(
        project_events=_get_project_events(project_path),
        disturb_lst=_read_disturb_list(
            os.path.join(cbm_input_dir, "disturb.lst")
        ),
        report_fil_data=parse_report_file(
            os.path.join(cbm_output_dir, "report.fil")
        ),
    )
