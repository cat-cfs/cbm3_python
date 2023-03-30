import numpy as np


def get_stock_changes_view(tfi):
    df = tfi.iloc[
        :, 0 : tfi.columns.get_loc("CO2Production")  # noqa E203
    ].copy()

    df["Delta Total Ecosystem"] = (
        tfi.GrossGrowth_AG
        + tfi.GrossGrowth_BG
        - (
            tfi.BioCO2Emission
            + tfi.BioCH4Emission
            + tfi.BioCOEmission
            + tfi.DOMCO2Emission
            + tfi.DOMCH4Emssion
            + tfi.DOMCOEmission
            + tfi.SoftProduction
            + tfi.HardProduction
            + tfi.DOMProduction
        )
    )
    df["Delta Total Biomass"] = (
        tfi.GrossGrowth_AG
        + tfi.GrossGrowth_BG
        - (
            tfi.BioCO2Emission
            + tfi.BioCH4Emission
            + tfi.BioCOEmission
            + tfi.SoftProduction
            + tfi.HardProduction
            + tfi.BiomassToSoil
        )
    )
    df["Delta Total DOM"] = (
        tfi.GrossGrowth_AG
        + tfi.GrossGrowth_BG
        - (
            tfi.DOMCO2Emission
            + tfi.DOMCH4Emssion
            + tfi.DOMCOEmission
            + tfi.DOMProduction
            + tfi.BiomassToSoil
        )
    )
    df["Net Primary Productivity (NPP)"] = np.where(
        tfi.DistTypeID == 0, tfi.GrossGrowth_AG + tfi.GrossGrowth_BG, 0.0
    )
    df["Net Ecosystem Productivity (NEP)"] = np.where(
        tfi.DistTypeID == 0,
        tfi.GrossGrowth_AG
        + tfi.GrossGrowth_BG
        - (tfi.DOMCO2Emission + tfi.DOMCH4Emssion + tfi.DOMCOEmission),
        0.0,
    )
    df["Net Growth"] = np.where(
        tfi.DistTypeID == 0, tfi.DeltaBiomass_AG + tfi.DeltaBiomass_BG, 0.0
    )
    df["Net Litterfall"] = np.where(tfi.DistTypeID == 0, tfi.DeltaDOM, 0.0)
    df["Total Litterfall"] = np.where(
        tfi.DistTypeID == 0, tfi.BiomassToSoil, 0.0
    )
    df["Decomposition Releases"] = np.where(
        tfi.DistTypeID == 0,
        (
            tfi.VFastAGToAir
            + tfi.VFastBGToAir
            + tfi.FastAGToAir
            + tfi.FastBGToAir
            + tfi.MediumToAir
            + tfi.SlowAGToAir
            + tfi.SlowBGToAir
            + tfi.SWStemSnagToAir
            + tfi.SWBranchSnagToAir
            + tfi.HWStemSnagToAir
            + tfi.HWBranchSnagToAir
            + tfi.BlackCarbonToAir
            + tfi.PeatToAir
        ),
        0.0,
    )
    df["NetCO2emissions_removals_CO2e"] = (
        tfi.GrossGrowth_AG
        + tfi.GrossGrowth_BG
        - tfi.DOMCO2Emission
        - tfi.BioCO2Emission
        - tfi.SoftProduction
        - tfi.HardProduction
        - tfi.DOMProduction
    ) * (-44 / 12)

    df["SumofCOProduction_CO2e"] = tfi.COProduction * 44 / 12
    df["SumofCH4Production_CO2e"] = tfi.CH4Production * 16 / 12 * 25
    df["N2O_CO2e"] = np.where(
        (tfi.CH4Production != 0.0) & (tfi.DistTypeID != 0),
        tfi.CO2Production * 44 / 12 * 0.00017 * 298,
        0.0,
    )
    df["ToFps_CO2e"] = (
        (tfi.SoftProduction + tfi.HardProduction + tfi.DOMProduction) * 44 / 12
    )

    df["Total Harvest (Biomass + Snags)"] = (
        tfi.SoftProduction + tfi.HardProduction + tfi.DOMProduction
    )

    df["Net forest-atmosphere exchange_CO2e"] = (
        df["NetCO2emissions_removals_CO2e"]
        - df["ToFps_CO2e"]
        + df["SumofCOProduction_CO2e"]
        + df["SumofCH4Production_CO2e"]
        + df["N2O_CO2e"]
    )

    df["Net forest-atmosphere exchange_C"] = (
        -df["Delta Total Ecosystem"] - df["Total Harvest (Biomass + Snags)"]
    )

    df["Total Harvest (Biomass)"] = tfi.SoftProduction + tfi.HardProduction
    df["Total Harvest (Snags)"] = tfi.DOMProduction
    df["Softwood Harvest (Biomass)"] = tfi.SoftProduction
    df["Hardwood Harvest (Biomass)"] = tfi.HardProduction
    df["Deadwood"] = (
        tfi.FastBGToAir
        + tfi.MediumToAir
        + tfi.SWStemSnagToAir
        + tfi.SWBranchSnagToAir
        + tfi.HWStemSnagToAir
        + tfi.HWBranchSnagToAir
    )
    df["Litter"] = tfi.FastAGToAir + tfi.VFastAGToAir + tfi.SlowAGToAir
    df["Soil C"] = tfi.VFastBGToAir + tfi.SlowBGToAir + tfi.BlackCarbonToAir
    df["Aboveground Very Fast DOM Emissions"] = tfi.VFastBGToAir
    df["Belowground Very Fast DOM Emissions"] = tfi.VFastAGToAir
    df["Aboveground Fast DOM Emissions"] = tfi.FastAGToAir
    df["Belowground Fast DOM Emissions"] = tfi.FastBGToAir
    df["Medium DOM Emissions"] = tfi.MediumToAir
    df["Aboveground Slow DOM Emissions"] = tfi.SlowAGToAir
    df["Belowground Slow DOM Emissions"] = tfi.SlowBGToAir
    df["Softwood Stem Snag Emissions"] = tfi.SWStemSnagToAir
    df["Softwood Branch Snag Emissions"] = tfi.SWBranchSnagToAir
    df["Hardwood Stem Snag Emissions"] = tfi.HWStemSnagToAir
    df["Hardwood Branch Snag Emissions"] = tfi.HWBranchSnagToAir
    df["Black Carbon Emissions"] = tfi.BlackCarbonToAir
    df["Peat Emissions"] = tfi.PeatToAir
    df["Biomass To DOM"] = (
        tfi.MerchLitterInput
        + tfi.FolLitterInput
        + tfi.OthLitterInput
        + tfi.CoarseLitterInput
        + tfi.FineLitterInput
    )
    df["Merchantable To DOM"] = tfi.MerchLitterInput
    df["Foliage To DOM"] = tfi.FolLitterInput
    df["Other To DOM"] = tfi.OthLitterInput
    df["Coarse Root To DOM"] = tfi.CoarseLitterInput
    df["Fine Root To DOM"] = tfi.FineLitterInput
    df["Total Emissions"] = (
        tfi.BioCO2Emission
        + tfi.BioCH4Emission
        + tfi.BioCOEmission
        + tfi.DOMCO2Emission
        + tfi.DOMCH4Emssion
        + tfi.DOMCOEmission
    )
    df["Total Biomass Emissions"] = (
        tfi.BioCO2Emission + tfi.BioCH4Emission + tfi.BioCOEmission
    )
    df["Total DOM Emissions"] = (
        tfi.DOMCO2Emission + tfi.DOMCH4Emssion + tfi.DOMCOEmission
    )
    df["Total CO2 Emissions"] = tfi.BioCO2Emission + tfi.DOMCO2Emission
    df["Total CO Emissions"] = tfi.BioCOEmission + tfi.DOMCOEmission
    df["Total CH4 Emissions"] = tfi.BioCH4Emission + tfi.DOMCH4Emssion
    df["Bio CO2 Emissions"] = tfi.BioCO2Emission
    df["Bio CO Emissions"] = tfi.BioCOEmission
    df["Bio CH4 Emissions"] = tfi.BioCH4Emission
    df["DOM CO2 Emissions"] = tfi.DOMCO2Emission
    df["DOM CO Emissions"] = tfi.DOMCOEmission
    df["DOM CH4 Emissions"] = tfi.DOMCH4Emssion
    df["Disturbance Losses"] = np.where(
        tfi.DistTypeID != 0,
        (
            tfi.BioCO2Emission
            + tfi.BioCH4Emission
            + tfi.BioCOEmission
            + tfi.DOMCO2Emission
            + tfi.DOMCH4Emssion
            + tfi.DOMCOEmission
            + tfi.SoftProduction
            + tfi.HardProduction
            + tfi.DOMProduction
        ),
        0.0,
    )
    df["Bio To Soil from Disturbances"] = np.where(
        tfi.DistTypeID != 0, tfi.BiomassToSoil, 0.0
    )
    df["Net Biome Productivity (NBP)"] = (
        tfi.GrossGrowth_AG
        + tfi.GrossGrowth_BG
        - (
            tfi.BioCO2Emission
            + tfi.BioCH4Emission
            + tfi.BioCOEmission
            + tfi.DOMCO2Emission
            + tfi.DOMCH4Emssion
            + tfi.DOMCOEmission
            + tfi.SoftProduction
            + tfi.HardProduction
            + tfi.DOMProduction
        )
    )

    return df
