import os
import pandas as pd


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
