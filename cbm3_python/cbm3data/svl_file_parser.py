from datetime import time
import os
import glob
from types import SimpleNamespace
import pandas as pd


def _iterate_svl_files(dir, n_timesteps):
    patterns = ["svl*", "spu*.dat"]
    for pattern in patterns:
        for path in glob.glob(os.path.join(os.path.abspath(dir), pattern)):
            base_path = os.path.basename(path)
            if os.path.splitext(base_path)[1].lower() == ".ini":
                timestep = 0
            elif "_" not in base_path:
                timestep = n_timesteps
            else:
                timestep = int(base_path.split(".")[0].split("_")[1])
            yield timestep, path


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


def _iterate_svl_lines(svl_file_path):

    dat_file = False
    if os.path.splitext(svl_file_path)[1].lower() == ".dat":
        dat_file = True

    with open(svl_file_path, "r") as fp:
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


def _build_col_def(*args):
    column_def = SimpleNamespace(column_names=[], column_types={})
    for arg in args:
        column_def.column_names.extend(arg["column_names"])
        column_def.column_types.update(
            {name: arg["column_type"] for name in arg["column_names"]}
        )
    return column_def


def _get_column_defintion():
    return _build_col_def(
        dict(column_names=["TimeStep"], column_type="int64"),
        dict(column_names=["SPUID"], column_type="int64"),
        dict(column_names=["Area"], column_type="float64"),
        dict(
            column_names=[
                "SVOID",
                "LastDisturbanceTypeID",
                "YearsSinceLastDisturbance",
            ],
            column_type="int64",
        ),
        # YearsSinceLUC this can be null
        dict(column_names=["YearsSinceLUC"], column_type="object"),
        dict(
            column_names=[
                "SWForestType",
                "SWGrowthCurveID",
                "SWManagementType",
                "SWMaturityState",
                "SWYearsInMaturityState",
                "SWAge",
            ],
            column_type="int64",
        ),
        dict(
            column_names=[
                "SWTotalBio_C_Density",
                "SWMerch_C_Density",
                "SWFoliage_C_Density",
                "SWSubMerch_C_Density",
                "SWOther_C_Density",
                "SWCoarseRoot_C_Density",
                "SWFineRoot_C_Density",
            ],
            column_type="float64",
        ),
        dict(
            column_names=[
                "HWForestType",
                "HWGrowthCurveID",
                "HWManagementType",
                "HWMaturityState",
                "HWYearsInMaturityState",
                "HWAge",
            ],
            column_type="int64",
        ),
        dict(
            column_names=[
                "HWTotalBio_C_Density",
                "HWMerch_C_Density",
                "HWFoliage_C_Density",
                "HWSubMerch_C_Density",
                "HWOther_C_Density",
                "HWCoarseRoot_C_Density",
                "HWFineRoot_C_Density",
                "TotalDOMC_Density",
                "VeryFastCAG_Density",
                "VeryFastCBG_Density",
                "FastCAG_Density",
                "FastCBG_Density",
                "MediumC_Density",
                "SlowCAG_Density",
                "SlowCBG_Density",
                "SWSSnagC_Density",
                "SWBSnagC_Density",
                "HWSSnagC_Density",
                "HWBSnagC_Density",
                "BlackC_Density",
                "PeatC_Density",
            ],
            column_type="float64",
        ),
        dict(
            column_names=[
                "c1",
                "c2",
                "c3",
                "c4",
                "c5",
                "c6",
                "c7",
                "c8",
                "c9",
                "c10",
                "landclass",
                "kf2",
                "kf3",
                "kf4",
                "kf5",
                "kf6",
            ],
            column_type="int64",
        ),
    )


def _typed_dataframe(col_def, data):
    df = pd.DataFrame(columns=col_def.column_names, data=data)

    for col_name in col_def.column_names:
        if col_name == "YearsSinceLUC":
            # fix this col since it is defined in the input svl files, but not
            # the output ones
            df["YearsSinceLUC"] = (
                df["YearsSinceLUC"]
                .astype("str")
                .str.strip()
                .replace("", -1)
                .astype("int64")
            )
        else:
            df[col_name] = df[col_name].astype(col_def.column_types[col_name])
    return df


def _add_timestep_column(svl_line_iterable, timestep):
    for line in svl_line_iterable:
        line.insert(0, timestep)
        yield line


def _parse_svl_files(dir, n_timesteps, chunksize=None):
    col_def = _get_column_defintion()
    lines = []
    for timestep, file in _iterate_svl_files(dir, n_timesteps):
        svl_line_iterable = _iterate_svl_lines(file)
        svl_line_iterable = _add_timestep_column(svl_line_iterable, timestep)
        if chunksize:
            for line in svl_line_iterable:
                lines.append(line)
                if len(lines) == chunksize:
                    yield _typed_dataframe(col_def, lines)
                    lines.clear()
        else:
            for line in svl_line_iterable:
                lines.append(line)
    yield _typed_dataframe(col_def, lines)


def _get_n_timesteps(input_dir):
    path = os.path.join(input_dir, "model.inf")
    with open(path) as model_inf_fp:
        token_count = 0
        for line in model_inf_fp:
            if line.startswith("#"):
                continue
            if token_count == 2:
                return int(line)
            token_count += 1


def _parse_all_chunked(input_dir, output_dir, chunksize):
    n_timesteps = _get_n_timesteps(input_dir)
    for df in _parse_svl_files(input_dir, n_timesteps, chunksize):
        yield df
    for df in _parse_svl_files(output_dir, n_timesteps, chunksize):
        yield df


def parse_all(input_dir, output_dir, chunksize=None):

    if chunksize:
        for chunk in _parse_all_chunked(input_dir, output_dir, chunksize):
            yield chunk
    else:
        chunks = _parse_all_chunked(input_dir, output_dir, None)
        out_data = pd.DataFrame()
        for chunk in chunks:
            out_data = out_data.append(chunk, ignore_index=True)
        yield out_data
