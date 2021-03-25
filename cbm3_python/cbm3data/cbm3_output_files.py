import os
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
        names=column_names, chunksize=chunksize)


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
        names=column_names, chunksize=chunksize)


def load_age_indicators(dir, chunksize=None):
    column_names = \
        ["RunID", "TimeStep", "SPUID", "AgeClass"] + \
        get_classifier_column_names() + \
        ["UNFCCC_ForestType", "KP33_34", "UNFCCC_Year", "KF33_Year",
         "KFProjectType", "KFProjectID", "Area", "Biomass_C", "DOM_C",
         "Avg_Age"]

    return pd.read_csv(
        os.path.join(dir, "ageind.out"), header=None, delim_whitespace=True,
        names=column_names, chunksize=chunksize)


def load_dist_indicators(dir, chunksize=None):
    column_names = \
        ["RunID", "TimeStep", "DistTypeID", "SPUID"] + \
        get_classifier_column_names() + \
        ["UNFCCC_ForestType", "KP33_34", "UNFCCC_Year", "KF33_Year",
         "KFProjectType", "KFProjectID", "DistArea", "DistProduct"]

    return pd.read_csv(
        os.path.join(dir, "distinds.out"), header=None, delim_whitespace=True,
        names=column_names, chunksize=chunksize)


def parse_svl_files(dir, chunksize=None):
    column_names = [
        "SPUID", "Area", "SVOID", "LastDisturbanceTypeID",
        "YearsSinceLastDisturbance", "YearsSinceLUC",

        "SWForestType", "SWGrowthCurveID", "SWManagementType",
        "SWMaturityState", "SWYearsInMaturityState", "SWAge",
        "SWTotalBio_C_Density", "SWMerch_C_Density", "SWFoliage_C_Density",
        "SWSubMerch_C_Density", "SWOther_C_density", "SWCoarseRoot_C_Density",
        "SWFineRoot_C_Density",

        "HWForestType", "HWGrowthCurveID", "HWManagementType",
        "HWMaturityState", "HWYearsInMaturityState", "HWAge",
        "HWTotalBio_C_Density", "HWMerch_C_Density", "HWFoliage_C_Density",
        "HWSubMerch_C_Density", "HWOther_C_density", "HWCoarseRoot_C_Density",
        "HWFineRoot_C_Density",

        "TotalDOMC_Density", "VeryFastCAG_Density", "VeryFastCBG_Density",
        "FastCAG_Density", "FastCBG_Density", "MediumC_Density",
        "SlowCAG_Density", "SlowCBG_Density", "SWSSnagC_Density",
        "SWBSnagC_Density", "HWSSnagC_Density", "HWBSnagC_Density",
        "BlackC_Density", "PeatC_Density",

        "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10",
        "landclass", "kf2", "kf3", "kf4", "kf5", "kf6", ]

    lines = []
    for file in svl_file_parser.iterate_svl_files(dir):
        print(file)
        svl_line_iterable = svl_file_parser.iterate_svl_lines(file)
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
