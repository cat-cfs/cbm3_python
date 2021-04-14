import os
import csv
import pandas as pd
from cbm3_python.cbm3data import svl_file_parser


def make_iterable(func, results_dir, chunksize=None):
    result = func(results_dir, chunksize)
    if chunksize:
        return result
    return [result]


def get_classifier_column_names():
    return [f"c{x}" for x in range(1, 11)]


def load_pool_indicators(dir, chunksize=None):
    column_names = ["RunID", "TimeStep", "SPUID"] + \
        get_classifier_column_names() + \
        ["UNFCCC_ForestType", "KP33_34", "UNFCCC_Year", "KF33_Year",
         "KFProjectType", "KFProjectID", "SWMerchC", "SWFoliageC", "SWOtherC",
         "SWSubmerchC", "SWCoarseRootC", "SWFineRootC", "HWMerchC",
         "HWFoliageC", "HWOtherC", "HWSubmerchC", "HWCoarseRootC",
         "HWFineRootC", "VeryFastCAG", "VeryFastCBG", "FastCAG", "FastCBG",
         "MediumC", "SlowCAG", "SlowCBG", "SWSSnagC", "SWBSnagC", "HWSSnagC",
         "HWBSnagC", "BlackC", "PeatC"]

    return pd.read_csv(
        os.path.join(dir, "poolind.out"), header=None, delim_whitespace=True,
        names=column_names, chunksize=chunksize, quoting=csv.QUOTE_NONE)


def load_flux_indicators(dir, chunksize=None):
    column_names = \
        ["RunID", "TimeStep", "DistTypeID", "SPUID"] + \
        get_classifier_column_names() + \
        ["UNFCCC_ForestType", "KP33_34", "UNFCCC_Year", "KF33_Year",
         "KFProjectType", "KFProjectID", "CO2Production", "CH4Production",
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
         "BioToAir_MERCHANTABLE",
         "BioToAir_FOLIAGE", "BioToAir_OTHER", "BioToAir_SUBMERCHANTABLE",
         "BioToAir_COARSEROOT", "BioToAir_FINEROOT"]

    return pd.read_csv(
        os.path.join(dir, "fluxind.out"), header=None, delim_whitespace=True,
        names=column_names, chunksize=chunksize, quoting=csv.QUOTE_NONE)


def load_age_indicators(dir, chunksize=None):
    column_names = \
        ["RunID", "TimeStep", "SPUID", "AgeClass"] + \
        get_classifier_column_names() + \
        ["UNFCCC_ForestType", "KP33_34", "UNFCCC_Year", "KF33_Year",
         "KFProjectType", "KFProjectID", "Area", "Biomass_C", "DOM_C",
         "Avg_Age"]

    return pd.read_csv(
        os.path.join(dir, "ageind.out"), header=None, delim_whitespace=True,
        names=column_names, chunksize=chunksize, quoting=csv.QUOTE_NONE)


def load_dist_indicators(dir, chunksize=None):
    column_names = \
        ["RunID", "TimeStep", "DistTypeID", "SPUID"] + \
        get_classifier_column_names() + \
        ["UNFCCC_ForestType", "KP33_34", "UNFCCC_Year", "KF33_Year",
         "KFProjectType", "KFProjectID", "DistArea", "DistProduct"]

    return pd.read_csv(
        os.path.join(dir, "distinds.out"), header=None, delim_whitespace=True,
        names=column_names, chunksize=chunksize, quoting=csv.QUOTE_NONE)


def load_svl_files(dir, chunksize=None):
    if chunksize:
        return svl_file_parser.parse_svl_files(dir, chunksize)
    else:
        return next(svl_file_parser.parse_svl_files(dir))


def load_nir_output(dir, chunksize=None):
    filename = "NIROutput.txt"
    column_names = [
        "TimeStep", "Year", "SPUID", "DistTypeID", "LandClass_From",
        "LandClass_To", "DisturbedArea", "SWMerchC", "SWFoliageC", "SWOtherC",
        "SWCoarseRootC", "SWFineRootC", "HWMerchC", "HWFoliageC", "HWOtherC",
        "HWCoarseRootC", "HWFineRootC", "VeryFastCAG", "VeryFastCBG",
        "FastCAG", "FastCBG", "MediumC", "SlowCAG", "SlowCBG", "SWSSnagC",
        "HWSSnagC", "SWBSnagC", "HWBSnagC", "BlackC", "PeatC"]
    return pd.read_csv(
        os.path.join(dir, filename), header=None, delim_whitespace=True,
        names=column_names, chunksize=chunksize, quoting=csv.QUOTE_NONE)


def load_nodist(dir, chunksize=None):
    filename = "nodist.fil"
    column_names = [
        "RunID", "TimeStep", "DistTypeID", "DistGroup", "UndisturbedArea"
    ]
    return pd.read_csv(
        os.path.join(dir, filename), header=None, delim_whitespace=True,
        names=column_names, chunksize=chunksize, quoting=csv.QUOTE_NONE)


def load_distseries(dir, chunksize=None):
    filename = "distseries.csv"
    # column headers are present in this csv file
    return pd.read_csv(
        os.path.join(dir, filename), chunksize=chunksize)


def load_accdiagnostics(dir, chunksize=None):
    filename = "accdiagnostics.txt"
    column_names = [
        "id", "rule_type", "target", "target_value", "TimeStep", "action",
        "DistTypeID", "area", "age"
    ]
    return pd.read_csv(
        os.path.join(dir, filename), header=None, delim_whitespace=True,
        names=column_names, chunksize=chunksize, quoting=csv.QUOTE_NONE)


def load_predistage(dir, chunksize=None):
    filename = "predistage.csv"
    # column headers are present in this csv file
    return pd.read_csv(
        os.path.join(dir, filename), chunksize=chunksize)


def load_seed(dir, chunksize=None):
    filename = "seed.txt"
    column_names = ["MonteCarloAssumptionID", "RunID", "RandomSeed"]
    return pd.read_csv(
        os.path.join(dir, filename), header=None, delim_whitespace=True,
        names=column_names, chunksize=chunksize, quoting=csv.QUOTE_NONE)


def load_spatial_pools(dir, chunksize=None):
    filename = "spatialpool.out"
    column_names = ["RunID", "SVOID", "Age", "TimeStep", "SPUID"] + \
        get_classifier_column_names() + \
        ["UNFCCC_ForestType", "KP33_34", "UNFCCC_Year", "KF33_Year",
         "KFProjectType", "KFProjectID", "SWMerchC", "SWFoliageC", "SWOtherC",
         "SWSubmerchC", "SWCoarseRootC", "SWFineRootC", "HWMerchC",
         "HWFoliageC", "HWOtherC", "HWSubmerchC", "HWCoarseRootC",
         "HWFineRootC", "VeryFastCAG", "VeryFastCBG", "FastCAG", "FastCBG",
         "MediumC", "SlowCAG", "SlowCBG", "SWSSnagC", "SWBSnagC", "HWSSnagC",
         "HWBSnagC", "BlackC", "PeatC"]
    return pd.read_csv(
        os.path.join(dir, filename), header=None, delim_whitespace=True,
        names=column_names, chunksize=chunksize, quoting=csv.QUOTE_NONE)


def load_spatial_flux(dir, chunksize=None):
    filename = "SpatialFluxInd.out"
    column_names = ["RunID", "SVOID", "TimeStep", "SPUID", "DistTypeID"] + \
        get_classifier_column_names() + \
        ["UNFCCC_ForestType", "KP33_34", "UNFCCC_Year", "KF33_Year",
         "KFProjectType", "KFProjectID", "CO2Production", "CH4Production",
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
         "BioToAir_FINEROOT"]
    return pd.read_csv(
        os.path.join(dir, filename), header=None, delim_whitespace=True,
        names=column_names, chunksize=chunksize, quoting=csv.QUOTE_NONE)
