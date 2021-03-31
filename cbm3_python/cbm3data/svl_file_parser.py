import os
import glob
import pandas as pd


def iterate_svl_files(dir):
    for path in glob.glob(os.path.join(os.path.abspath(dir), "svl*")):
        yield path


def _process_type(t):
    try:
        return int(t)
    except ValueError:
        try:
            return float(t)
        except ValueError:
            return t


def _process_token_types(tokens):
    for i, t in enumerate(tokens):
        tokens[i] = _process_type(t)
    return tokens


def iterate_svl_lines(svl_file_path):

    dat_file = False
    if os.path.splitext(svl_file_path)[1].lower() == ".dat":
        dat_file = True

    with open(svl_file_path, 'r') as fp:
        if dat_file:
            for i_line, line in enumerate(fp):
                if i_line == 0:  # first line is not part of the data
                    continue
                else:
                    tokens = line.split()
                    # insert a null-value token to compensate for a field
                    # "years_since_land_use_change"
                    # found in ini format but not dat format
                    tokens.insert(5, "")
                    yield _process_token_types(tokens)
        else:
            line_tokens = []
            for i_line, line in enumerate(fp):
                if (i_line % 6) == 0:
                    if not i_line == 0:
                        yield _process_token_types(line_tokens)
                        line_tokens = []
                elif (i_line % 6) == 5:
                    tokens = line.split()
                    num_classifiers = len(tokens) - 6
                    needed_classifiers = 10 - num_classifiers
                    # need to insert null classifier values (-99)
                    # which appear in .dat but not in .ini format
                    line_tokens.extend(tokens[:num_classifiers])
                    line_tokens.extend([-99] * needed_classifiers)
                    line_tokens.extend(tokens[num_classifiers:])
                else:
                    line_tokens.extend(line.split())
            if line_tokens:
                yield _process_token_types(line_tokens)


def parse_svl_files(dir, chunksize=None):
    column_names = [
        "SPUID", "Area", "SVOID", "LastDisturbanceTypeID",
        "YearsSinceLastDisturbance", "YearsSinceLUC",

        "SWForestType", "SWGrowthCurveID", "SWManagementType",
        "SWMaturityState", "SWYearsInMaturityState", "SWAge",
        "SWTotalBio_C_Density", "SWMerch_C_Density", "SWFoliage_C_Density",
        "SWSubMerch_C_Density", "SWOther_C_Density", "SWCoarseRoot_C_Density",
        "SWFineRoot_C_Density",

        "HWForestType", "HWGrowthCurveID", "HWManagementType",
        "HWMaturityState", "HWYearsInMaturityState", "HWAge",
        "HWTotalBio_C_Density", "HWMerch_C_Density", "HWFoliage_C_Density",
        "HWSubMerch_C_Density", "HWOther_C_Density", "HWCoarseRoot_C_Density",
        "HWFineRoot_C_Density",

        "TotalDOMC_Density", "VeryFastCAG_Density", "VeryFastCBG_Density",
        "FastCAG_Density", "FastCBG_Density", "MediumC_Density",
        "SlowCAG_Density", "SlowCBG_Density", "SWSSnagC_Density",
        "SWBSnagC_Density", "HWSSnagC_Density", "HWBSnagC_Density",
        "BlackC_Density", "PeatC_Density",

        "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10",
        "landclass", "kf2", "kf3", "kf4", "kf5", "kf6", ]

    lines = []
    for file in iterate_svl_files(dir):
        svl_line_iterable = iterate_svl_lines(file)
        if chunksize:
            for line in svl_line_iterable:
                lines.append(line)
                if len(lines) == chunksize:
                    yield pd.DataFrame(
                        columns=column_names, data=lines)
                    lines.clear()
        else:
            for line in svl_line_iterable:
                lines.append(line)
    yield pd.DataFrame(
        columns=column_names, data=lines)
