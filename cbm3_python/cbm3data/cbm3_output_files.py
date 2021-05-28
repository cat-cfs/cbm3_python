import os
import csv
import pandas as pd
from types import SimpleNamespace
from cbm3_python.cbm3data import svl_file_parser


def _typed_dataframe(col_def, data):
    df = pd.DataFrame(columns=col_def.column_names, data=data)

    for col_name in col_def.column_names:
        df[col_name] = df[col_name].astype(col_def.column_types[col_name])
    return df


def _yield_empty_dataframe(col_def, chunksize=None):
    df = _typed_dataframe(col_def, None)
    if chunksize:
        return [df]
    else:
        return df


def _build_col_def(*args):
    column_def = SimpleNamespace(
        column_names=[],
        column_types={}
    )
    for arg in args:
        column_def.column_names.extend(
            arg["column_names"])
        column_def.column_types.update(
            {name: arg["column_type"] for name in arg["column_names"]})
    return column_def


def get_classifier_column_names():
    """Get headers for the 10 classifier columns found in many CBM output
    files.

    Returns:
        list: list of strings ["c1", "c2", ... "c10"]
    """
    return [f"c{x}" for x in range(1, 11)]


def load_pool_indicators(dir, chunksize=None):
    """load cbmrun/output/poolind.out to a pandas.DataFrame

    Args:
        dir (str): path to the CBMRun/output dir
        chunksize (int, optional): If specified sets a maximum number of rows
            to hold in memory at a given time while loading output.
            Defaults to None.

    Returns:
        pandas.DataFrame, or object: returns an iterable of dataframes
            if chunksize is specified, and otherwise a single dataframe.
    """
    col_def = _build_col_def(
        dict(column_names=["RunID", "TimeStep", "SPUID"],
             column_type="int64"),
        dict(column_names=get_classifier_column_names(),
             column_type="int64"),
        dict(column_names=[
            "UNFCCC_ForestType", "KP33_34", "UNFCCC_Year", "KF33_Year",
            "KFProjectType", "KFProjectID"], column_type="int64"),
        dict(column_names=[
            "SWMerchC", "SWFoliageC", "SWOtherC",
            "SWSubmerchC", "SWCoarseRootC", "SWFineRootC", "HWMerchC",
            "HWFoliageC", "HWOtherC", "HWSubmerchC", "HWCoarseRootC",
            "HWFineRootC", "VeryFastCAG", "VeryFastCBG", "FastCAG", "FastCBG",
            "MediumC", "SlowCAG", "SlowCBG", "SWSSnagC", "SWBSnagC",
            "HWSSnagC", "HWBSnagC", "BlackC", "PeatC"],
             column_type="float64"))

    return pd.read_csv(
        os.path.join(dir, "poolind.out"), header=None, delim_whitespace=True,
        names=col_def.column_names, dtype=col_def.column_types,
        chunksize=chunksize, quoting=csv.QUOTE_NONE)


def load_flux_indicators(dir, chunksize=None):
    """load cbmrun/output/fluxind.out to a pandas.DataFrame

    Args:
        dir (str): path to the CBMRun/output dir
        chunksize (int, optional): If specified sets a maximum number of rows
            to hold in memory at a given time while loading output.
            Defaults to None.

    Returns:
        pandas.DataFrame, or object: returns an iterable of dataframes
            if chunksize is specified, and otherwise a single dataframe.
    """
    col_def = _build_col_def(
        dict(column_names=["RunID", "TimeStep", "DistTypeID", "SPUID"],
             column_type="int64"),
        dict(column_names=get_classifier_column_names(), column_type="int64"),
        dict(column_names=[
            "UNFCCC_ForestType", "KP33_34", "UNFCCC_Year", "KF33_Year",
            "KFProjectType", "KFProjectID"], column_type="int64"),
        dict(column_names=[
            "CO2Production", "CH4Production", "COProduction",
            "BioCO2Emission", "BioCH4Emission", "BioCOEmission",
            "DOMCO2Emission", "DOMCH4Emission", "DOMCOEmission",
            "SoftProduction", "HardProduction", "DOMProduction",
            "DeltaBiomass_AG", "DeltaBiomass_BG", "DeltaDOM", "BiomassToSoil",
            "LitterFlux_MERCHANTABLE", "LitterFlux_FOLIAGE",
            "LitterFlux_OTHER", "LitterFlux_SUBMERCHANTABLE",
            "LitterFlux_COARSEROOT", "LitterFlux_FINEROOT",
            "SoilToAir_VERYFASTAG", "SoilToAir_VERYFASTBG",
            "SoilToAir_FASTAG", "SoilToAir_FASTBG", "SoilToAir_MEDIUM",
            "SoilToAir_SLOWAG", "SoilToAir_SLOWBG", "SoilToAir_SSTEMSNAG",
            "SoilToAir_SBRANCHSNAG", "SoilToAir_HSTEMSNAG",
            "SoilToAir_HBRANCHSNAG", "SoilToAir_BLACKCARBON",
            "SoilToAir_PEAT", "BioToAir_MERCHANTABLE", "BioToAir_FOLIAGE",
            "BioToAir_OTHER", "BioToAir_SUBMERCHANTABLE",
            "BioToAir_COARSEROOT", "BioToAir_FINEROOT"],
            column_type="float64"))

    return pd.read_csv(
        os.path.join(dir, "fluxind.out"), header=None, delim_whitespace=True,
        names=col_def.column_names, dtype=col_def.column_types,
        chunksize=chunksize, quoting=csv.QUOTE_NONE)


def load_age_indicators(dir, chunksize=None):
    """load cbmrun/output/ageind.out to a pandas.DataFrame

    Args:
        dir (str): path to the CBMRun/output dir
        chunksize (int, optional): If specified sets a maximum number of rows
            to hold in memory at a given time while loading output.
            Defaults to None.

    Returns:
        pandas.DataFrame, or object: returns an iterable of dataframes
            if chunksize is specified, and otherwise a single dataframe.
    """
    col_def = _build_col_def(
        dict(column_names=["RunID", "TimeStep", "SPUID", "AgeClass"],
             column_type="int64"),
        dict(column_names=get_classifier_column_names(), column_type="int64"),
        dict(column_names=[
            "UNFCCC_ForestType", "KP33_34", "UNFCCC_Year", "KF33_Year",
            "KFProjectType", "KFProjectID"], column_type="int64"),
        dict(column_names=["Area", "Biomass_C", "DOM_C", "Avg_Age"],
             column_type="float64"))

    return pd.read_csv(
        os.path.join(dir, "ageind.out"), header=None, delim_whitespace=True,
        names=col_def.column_names, dtype=col_def.column_types,
        chunksize=chunksize, quoting=csv.QUOTE_NONE)


def load_dist_indicators(dir, chunksize=None):
    """load cbmrun/output/distinds.out to a pandas.DataFrame

    Args:
        dir (str): path to the CBMRun/output dir
        chunksize (int, optional): If specified sets a maximum number of rows
            to hold in memory at a given time while loading output.
            Defaults to None.

    Returns:
        pandas.DataFrame, or object: returns an iterable of dataframes
            if chunksize is specified, and otherwise a single dataframe.
    """
    col_def = _build_col_def(
        dict(column_names=["RunID", "TimeStep", "DistTypeID", "SPUID"],
             column_type="int64"),
        dict(column_names=get_classifier_column_names(),
             column_type="int64"),
        dict(column_names=[
            "UNFCCC_ForestType", "KP33_34", "UNFCCC_Year", "KF33_Year",
            "KFProjectType", "KFProjectID"], column_type="int64"),
        dict(column_names=["DistArea", "DistProduct"], column_type="float64"))

    return pd.read_csv(
        os.path.join(dir, "distinds.out"), header=None, delim_whitespace=True,
        names=col_def.column_names, dtype=col_def.column_types,
        chunksize=chunksize, quoting=csv.QUOTE_NONE)


def load_svl_files(input_dir, output_dir, chunksize=None):
    """load cbmrun/output/svl***.dat and cbmrun/input/svl***.ini files
    to a pandas.DataFrame

    Args:
        input_dir (str): path to the CBMRun/input dir
        output_dir (str): path to the CBMRun/output dir
        chunksize (int, optional): If specified sets a maximum number of rows
            to hold in memory at a given time while loading output.
            Defaults to None.

    Returns:
        pandas.DataFrame, or object: returns an iterable of dataframes
            if chunksize is specified, and otherwise a single dataframe.
    """
    result = svl_file_parser.parse_all(input_dir, output_dir, chunksize)
    if chunksize:
        return result
    else:
        return next(result)


def load_nir_output(dir, chunksize=None):
    """load cbmrun/output/NIROutput.txt to a pandas.DataFrame

    Args:
        dir (str): path to the CBMRun/output dir
        chunksize (int, optional): If specified sets a maximum number of rows
            to hold in memory at a given time while loading output.
            Defaults to None.

    Returns:
        pandas.DataFrame, or object: returns an iterable of dataframes
            if chunksize is specified, and otherwise a single dataframe.
    """
    filename = "NIROutput.txt"
    col_def = _build_col_def(
        dict(column_names=[
            "TimeStep", "Year", "SPUID", "DistTypeID", "LandClass_From",
            "LandClass_To"], column_type="int64"),
        dict(column_names=[
            "DisturbedArea", "SWMerchC", "SWFoliageC",
            "SWOtherC", "SWCoarseRootC", "SWFineRootC", "HWMerchC",
            "HWFoliageC", "HWOtherC", "HWCoarseRootC", "HWFineRootC",
            "VeryFastCAG", "VeryFastCBG", "FastCAG", "FastCBG", "MediumC",
            "SlowCAG", "SlowCBG", "SWSSnagC", "HWSSnagC", "SWBSnagC",
            "HWBSnagC", "BlackC", "PeatC"], column_type="float64"))
    return pd.read_csv(
        os.path.join(dir, filename), header=None, delim_whitespace=True,
        names=col_def.column_names, dtype=col_def.column_types,
        chunksize=chunksize, quoting=csv.QUOTE_NONE)


def load_nodist(dir, chunksize=None):
    """load cbmrun/output/nodist.out to a pandas.DataFrame

    Args:
        dir (str): path to the CBMRun/output dir
        chunksize (int, optional): If specified sets a maximum number of rows
            to hold in memory at a given time while loading output.
            Defaults to None.

    Returns:
        pandas.DataFrame, or object: returns an iterable of dataframes
            if chunksize is specified, and otherwise a single dataframe.
    """
    filename = "nodist.fil"
    col_def = _build_col_def(
        dict(column_names=["RunID", "TimeStep", "DistTypeID", "DistGroup"],
             column_type="int64"),
        dict(column_names=["UndisturbedArea"], column_type="float64"))
    return pd.read_csv(
        os.path.join(dir, filename), header=None, delim_whitespace=True,
        names=col_def.column_names, dtype=col_def.column_types,
        chunksize=chunksize, quoting=csv.QUOTE_NONE)


def load_distseries(dir, chunksize=None):
    """load cbmrun/output/distseries.csv to a pandas.DataFrame

    Args:
        dir (str): path to the CBMRun/output dir
        chunksize (int, optional): If specified sets a maximum number of rows
            to hold in memory at a given time while loading output.
            Defaults to None.

    Returns:
        pandas.DataFrame, or object: returns an iterable of dataframes
            if chunksize is specified, and otherwise a single dataframe.
    """
    filename = "distseries.csv"
    # column headers are present in this csv file
    col_def = _build_col_def(
        dict(column_names=["timestep", "previous_kf5", "current_kf5"],
             column_type="int64"),
        dict(column_names=["area_disturbed"], column_type="float64"))
    return pd.read_csv(
        os.path.join(dir, filename), header=0,
        names=col_def.column_names, dtype=col_def.column_types,
        chunksize=chunksize)


def load_accdiagnostics(dir, chunksize=None):
    """load cbmrun/output/accdiagnostics.txt to a pandas.DataFrame

    Args:
        dir (str): path to the CBMRun/output dir
        chunksize (int, optional): If specified sets a maximum number of rows
            to hold in memory at a given time while loading output.
            Defaults to None.

    Returns:
        pandas.DataFrame, or object: returns an iterable of dataframes
            if chunksize is specified, and otherwise a single dataframe.
    """
    filename = "accdiagnostics.txt"
    col_def = _build_col_def(
        dict(column_names=["id"], column_type="int64"),
        dict(column_names=["rule_type"], column_type="object"),
        dict(column_names=["target", "target_value"], column_type="float64"),
        dict(column_names=["TimeStep"], column_type="int64"),
        dict(column_names=["action"], column_type="object"),
        dict(column_names=["DistTypeID"], column_type="int64"),
        dict(column_names=["area"], column_type="float64"),
        dict(column_names=["age"], column_type="int64")
    )
    return pd.read_csv(
        os.path.join(dir, filename), header=None, names=col_def.column_names,
        dtype=col_def.column_types, chunksize=chunksize, quotechar="'")


def load_predistage(dir, chunksize=None):
    """load cbmrun/output/predistage.csv to a pandas.DataFrame

    Args:
        dir (str): path to the CBMRun/output dir
        chunksize (int, optional): If specified sets a maximum number of rows
            to hold in memory at a given time while loading output.
            Defaults to None.

    Returns:
        pandas.DataFrame, or object: returns an iterable of dataframes
            if chunksize is specified, and otherwise a single dataframe.
    """
    filename = "predistage.csv"
    # column headers are present in this csv file
    col_def = _build_col_def(
        dict(column_names=[
            "spuid", "dist_type", "timestep", "c0", "c1", "c2", "c3", "c4",
            "c5", "c6", "c7", "c8", "c9"], column_type="int64"),
        dict(column_names=["empty"], column_type="float64"),
        dict(column_names=["k0", "k1", "k2", "k3", "k4", "k5", "pre_dist_age"],
             column_type="int64"),
        dict(column_names=["area_disturbed"], column_type="float64"))
    df = pd.read_csv(
        os.path.join(dir, filename), header=0,
        names=col_def.column_names, dtype=col_def.column_types,
        chunksize=chunksize, index_col=False)
    #  index_col=False here solves an issue where pandas
    #  doesn't parse columns well when there is an extra
    #  trailing column in the data

    return df


def load_seed(dir, chunksize=None):
    """load cbmrun/output/seed.txt to a pandas.DataFrame. If the file is not
    present an empty dataframe is returned.

    Args:
        dir (str): path to the CBMRun/output dir
        chunksize (int, optional): If specified sets a maximum number of rows
            to hold in memory at a given time while loading output.
            Defaults to None.

    Returns:
        pandas.DataFrame, or object: returns an iterable of dataframes
            if chunksize is specified, and otherwise a single dataframe.
    """
    filename = "seed.txt"
    path = os.path.join(dir, filename)
    col_def = _build_col_def(
        dict(column_names=["MonteCarloAssumptionID", "RunID", "RandomSeed"],
             column_type="int64"))

    if not os.path.exists(path):
        return _yield_empty_dataframe(col_def, chunksize)
    else:
        return pd.read_csv(
            path, header=None, delim_whitespace=True,
            names=col_def.column_names, dtype=col_def.column_types,
            chunksize=chunksize, quoting=csv.QUOTE_NONE)


def load_spatial_pools(dir, chunksize=None):
    """load cbmrun/output/spatialpool.out to a pandas.DataFrame.

    Args:
        dir (str): path to the CBMRun/output dir
        chunksize (int, optional): If specified sets a maximum number of rows
            to hold in memory at a given time while loading output.
            Defaults to None.

    Returns:
        pandas.DataFrame, or object: returns an iterable of dataframes
            if chunksize is specified, and otherwise a single dataframe.
    """
    filename = "spatialpool.out"
    col_def = _build_col_def(
        dict(column_names=["RunID", "SVOID", "Age", "TimeStep", "SPUID"],
             column_type="int64"),
        dict(column_names=get_classifier_column_names(), column_type="int64"),
        dict(column_names=[
            "UNFCCC_ForestType", "KP33_34", "UNFCCC_Year", "KF33_Year",
            "KFProjectType", "KFProjectID"], column_type="int64"),
        dict(column_names=[
         "SWMerchC", "SWFoliageC", "SWOtherC",
         "SWSubmerchC", "SWCoarseRootC", "SWFineRootC", "HWMerchC",
         "HWFoliageC", "HWOtherC", "HWSubmerchC", "HWCoarseRootC",
         "HWFineRootC", "VeryFastCAG", "VeryFastCBG", "FastCAG", "FastCBG",
         "MediumC", "SlowCAG", "SlowCBG", "SWSSnagC", "SWBSnagC", "HWSSnagC",
         "HWBSnagC", "BlackC", "PeatC"], column_type="float64"))

    return pd.read_csv(
        os.path.join(dir, filename), header=None, delim_whitespace=True,
        names=col_def.column_names, dtype=col_def.column_types,
        chunksize=chunksize, quoting=csv.QUOTE_NONE)


def load_spatial_flux(dir, chunksize=None):
    """load cbmrun/output/SpatialFluxInd.out to a pandas.DataFrame. If the
    file is not present an empty dataframe is returned.

    Args:
        dir (str): path to the CBMRun/output dir
        chunksize (int, optional): If specified sets a maximum number of rows
            to hold in memory at a given time while loading output.
            Defaults to None.

    Returns:
        pandas.DataFrame, or object: returns an iterable of dataframes
            if chunksize is specified, and otherwise a single dataframe.
    """
    filename = "SpatialFluxInd.out"
    col_def = _build_col_def(
        dict(column_names=[
            "RunID", "SVOID", "TimeStep", "SPUID", "DistTypeID"],
            column_type="int64"),
        dict(column_names=get_classifier_column_names(),
             column_type="int64"),
        dict(column_names=[
             "UNFCCC_ForestType", "KP33_34", "UNFCCC_Year",
             "KF33_Year", "KFProjectType", "KFProjectID"],
             column_type="int64"),
        dict(column_names=[
         "CO2Production", "CH4Production",
         "COProduction", "BioCO2Emission", "BioCH4Emission", "BioCOEmission",
         "DOMCO2Emission", "DOMCH4Emission", "DOMCOEmission", "SoftProduction",
         "HardProduction", "DOMProduction", "DeltaBiomass_AG",
         "DeltaBiomass_BG", "DeltaDOM", "BiomassToSoil",
         "LitterFlux_MERCHANTABLE", "LitterFlux_FOLIAGE", "LitterFlux_OTHER",
         "LitterFlux_SUBMERCHANTABLE", "LitterFlux_COARSEROOT",
         "LitterFlux_FINEROOT", "SoilToAir_VERYFASTAG", "SoilToAir_VERYFASTBG",
         "SoilToAir_FASTAG", "SoilToAir_FASTBG", "SoilToAir_MEDIUM",
         "SoilToAir_SLOWAG", "SoilToAir_SLOWBG", "SoilToAir_SSTEMSNAG",
         "SoilToAir_SBRANCHSNAG", "SoilToAir_HSTEMSNAG",
         "SoilToAir_HBRANCHSNAG", "SoilToAir_BLACKCARBON", "SoilToAir_PEAT",
         "BioToAir_MERCHANTABLE", "BioToAir_FOLIAGE", "BioToAir_OTHER",
         "BioToAir_SUBMERCHANTABLE", "BioToAir_COARSEROOT",
         "BioToAir_FINEROOT"], column_type="float64"))
    file_path = os.path.join(dir, filename)

    if not os.path.exists(file_path):
        return _yield_empty_dataframe(col_def, chunksize)
    else:
        return pd.read_csv(
            file_path, header=None, delim_whitespace=True,
            names=col_def.column_names, dtype=col_def.column_types,
            chunksize=chunksize, quoting=csv.QUOTE_NONE)
