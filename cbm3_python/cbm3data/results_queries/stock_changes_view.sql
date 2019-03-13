select
tfi.TimeStep,{0}
sum(tfi.GrossGrowth_AG+tfi.GrossGrowth_BG-
    (tfi.BioCO2Emission+tfi.BioCH4Emission+tfi.BioCOEmission+
     tfi.DOMCO2Emission+tfi.DOMCH4Emssion+tfi.DOMCOEmission+
     tfi.SoftProduction+tfi.HardProduction+tfi.DOMProduction))
     as [Delta Total Ecosystem],
sum(tfi.GrossGrowth_AG+tfi.GrossGrowth_BG-
    (tfi.BioCO2Emission+tfi.BioCH4Emission+tfi.BioCOEmission+
     tfi.SoftProduction+tfi.HardProduction + tfi.BiomassToSoil))
     as [Delta Total Biomass],
sum(tfi.GrossGrowth_AG+tfi.GrossGrowth_BG-
    (tfi.DOMCO2Emission+tfi.DOMCH4Emssion+tfi.DOMCOEmission+
     tfi.DOMProduction + tfi.BiomassToSoil)) as [Delta Total DOM],
sum(iif(tfi.DistTypeID=0, tfi.GrossGrowth_AG+tfi.GrossGrowth_BG,0))
    as [Net Primary Productivity (NPP)],
sum(iif(tfi.DistTypeID=0, tfi.GrossGrowth_AG+tfi.GrossGrowth_BG -
    (tfi.DOMCO2Emission+tfi.DOMCH4Emssion+tfi.DOMCOEmission),0))
    as [Net Ecosystem Productivity (NEP)],
sum(iif(tfi.DistTypeID=0,tfi.DeltaBiomass_AG+tfi.DeltaBiomass_BG,0))
    as [Net Growth],
sum(iif(tfi.DistTypeID=0,tfi.DeltaDOM,0)) as [Net Litterfall],
sum(iif(tfi.DistTypeID=0,tfi.BiomassToSoil,0)) as [Total Litterfall],
sum(iif(tfi.DistTypeID=0, 
    tfi.VFastAGToAir+tfi.VFastBGToAir+tfi.FastAGToAir+tfi.FastBGToAir+
    tfi.MediumToAir+tfi.SlowAGToAir+tfi.SlowBGToAir+tfi.SWStemSnagToAir+
    tfi.SWBranchSnagToAir+tfi.HWStemSnagToAir+tfi.HWBranchSnagToAir+
    tfi.BlackCarbonToAir+tfi.PeatToAir, 0)) as [Decomposition Releases],
sum(tfi.GrossGrowth_AG+tfi.GrossGrowth_BG
    -tfi.DOMCO2Emission-tfi.BioCO2Emission)*(-44/12)
    as [NetCO2emissions_removals_CO2e],
sum(tfi.COProduction*44/12) AS [SumofCOProduction_CO2e],
sum(tfi.CH4Production*16/12) AS [SumofCH4Production_CO2e],
sum(iif(tfi.CH4production<>0 and tfi.DistTypeID<>0, 
    tfi.CO2production*44/12*0.00017*310, 0)) AS [N2O_CO2e],
sum((tfi.SoftProduction+tfi.HardProduction+tfi.DOMProduction)*44/12)
    as [ToFps_CO2e],
sum(tfi.SoftProduction+tfi.HardProduction+tfi.DOMProduction) 
    as [Total Harvest (Biomass + Snags)],
sum(tfi.SoftProduction+tfi.HardProduction) as [Total Harvest (Biomass)],
sum(tfi.DOMProduction) as [Total Harvest (Snags)],
sum(tfi.SoftProduction) as [Softwood Harvest (Biomass)],
sum(tfi.HardProduction) as [Hardwood Harvest (Biomass)],
sum(tfi.FastAGToAir+tfi.FastBGToAir+tfi.MediumToAir+tfi.SWStemSnagToAir
    +tfi.SWBranchSnagToAir+tfi.HWStemSnagToAir+tfi.HWBranchSnagToAir)
    as [Deadwood],
sum(tfi.VFastAGToAir+tfi.SlowAGToAir) as [Litter],
sum(tfi.VFastBGToAir + tfi.SlowBGToAir + tfi.BlackCarbonToAir) as [Soil C],
sum(tfi.VFastBGToAir) as [Aboveground Very Fast DOM Emissions],
sum(tfi.VFastAGToAir) as [Belowground Very Fast DOM Emissions],
sum(tfi.FastAGToAir) as [Aboveground Fast DOM Emissions],
sum(tfi.FastBGToAir) as [Belowground Fast DOM Emissions],
sum(tfi.MediumToAir) as [Medium DOM Emissions],
sum(tfi.SlowAGToAir) as [Aboveground Slow DOM Emissions],
sum(tfi.SlowBGToAir) as [Belowground Slow DOM Emissions],
sum(tfi.SWStemSnagToAir) as [Softwood Stem Snag Emissions],
sum(tfi.SWBranchSnagToAir) as [Softwood Branch Snag Emissions],
sum(tfi.HWStemSnagToAir) as [Hardwood Stem Snag Emissions],
sum(tfi.HWBranchSnagToAir) as [Hardwood Branch Snag Emissions],
sum(tfi.BlackCarbonToAir) as [Black Carbon Emissions],
sum(tfi.PeatToAir) as [Peat Emissions],
sum(tfi.MerchLitterInput+tfi.FolLitterInput+tfi.OthLitterInput
    +tfi.CoarseLitterInput+tfi.FineLitterInput) as [Biomass To DOM],
sum(tfi.MerchLitterInput) as [Merchantable To DOM],
sum(tfi.FolLitterInput) as [Foliage To DOM],
sum(tfi.OthLitterInput) as [Other To DOM],
sum(tfi.CoarseLitterInput) as [Coarse Root To DOM],
sum(tfi.FineLitterInput) as [Fine Root To DOM],
sum(tfi.BioCO2Emission+tfi.BioCH4Emission+tfi.BioCOEmission
    +tfi.DOMCO2Emission+tfi.DOMCH4Emssion+tfi.DOMCOEmission)
    as [Total Emissions],
sum(tfi.BioCO2Emission+tfi.BioCH4Emission+tfi.BioCOEmission)
    as [Total Biomass Emissions],
sum(tfi.DOMCO2Emission+tfi.DOMCH4Emssion+tfi.DOMCOEmission)
    as [Total DOM Emissions],
sum(tfi.BioCO2Emission+tfi.DOMCO2Emission) as [Total CO2 Emissions],
sum(tfi.BioCOEmission+tfi.DOMCOEmission) as [Total CO Emissions],
sum(tfi.BioCH4Emission+tfi.DOMCH4Emssion) as [Total CH4 Emissions],
sum(tfi.BioCO2Emission) as [Bio CO2 Emissions],
sum(tfi.BioCOEmission) as [Bio CO Emissions],
sum(tfi.BioCH4Emission) as [Bio CH4 Emissions],
sum(tfi.DOMCO2Emission) as [DOM CO2 Emissions],
sum(tfi.DOMCOEmission) as [DOM CO Emissions],
sum(tfi.DOMCH4Emssion) as [DOM CH4 Emissions],
sum(iif(tfi.DistTypeID<>0, tfi.BioCO2Emission+tfi.BioCH4Emission
    +tfi.BioCOEmission+tfi.DOMCO2Emission+tfi.DOMCH4Emssion
    +tfi.DOMCOEmission+tfi.SoftProduction+tfi.HardProduction
    +tfi.DOMProduction, 0)) as [Disturbance Losses],
sum(iif(tfi.DistTypeID<>0, tfi.BiomassToSoil, 0))
    as [Bio To Soil from Disturbances],
sum(tfi.GrossGrowth_AG+tfi.GrossGrowth_BG-
    (tfi.BioCO2Emission+tfi.BioCH4Emission
     +tfi.BioCOEmission+tfi.DOMCO2Emission+tfi.DOMCH4Emssion
     +tfi.DOMCOEmission+tfi.SoftProduction+tfi.HardProduction
     +tfi.DOMProduction)) as [Net Biome Productivity (NBP)]
from tblFluxIndicators tfi
group by tfi.TimeStep{1}
order by tfi.TimeStep{1}