# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

# This script's purpose is to mine the CBM-CFS3 report.fil for its
# "Disturbance Reconciliation" output

import re
import pandas as pd
import numpy as np


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
    with open(report_fil_path, 'r') as fp:
        return _parse_report_fil(fp)


def _parse_report_fil(inputFile):

    fileContents = inputFile.read()
    matches = re.findall(
        'Disturbance Reconciliation.+?Records Changed:[ \t0-9]+',
        fileContents, re.DOTALL)
    # the 'Disturbance Reconciliation' part matches the first bit of text in
    # each block '.+?' matches any character in a non greedy fashion
    # 'Records Changed:[ \t0-9]+' matches the end of each block

    # variable to hold the column names and indexes
    columns = []

    # loop 1, create the collection of names in the Disturbance Reconciliation
    # blocks
    for match in matches:
        for innerMatch in re.split('[\r\n]', match):
            if 'Reconciliation' in innerMatch:
                continue
            kvp = re.split('    |[:]', innerMatch)
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
        for innerMatch in re.split('[\r\n]', match):
            if 'Reconciliation' in innerMatch:
                continue
            kvp = re.split('    |[:]', innerMatch)
            kvp = list(filter(None, kvp))
            col = kvp[0].strip()
            value = float(kvp[1].strip())
            row[col] = value
        for col, value in row.items():
            table[col].append(value)

    return pd.DataFrame(data=table)
