select
tfi.TimeStep,
sum(tfi.GrossGrowth_AG+tfi.GrossGrowth_BG-
    (tfi.BioCO2Emission+tfi.BioCH4Emission+tfi.BioCOEmission+
     tfi.DOMCO2Emission+tfi.DOMCH4Emssion+tfi.DOMCOEmission+
     tfi.SoftProduction+tfi.HardProduction+tfi.DOMProduction)) as [Delta Total Ecosystem],
sum(tfi.GrossGrowth_AG+tfi.GrossGrowth_BG-
    (tfi.BioCO2Emission+tfi.BioCH4Emission+tfi.BioCOEmission+
     tfi.SoftProduction+tfi.HardProduction + tfi.BiomassToSoil)) as [Delta Total Biomass],
sum(tfi.GrossGrowth_AG+tfi.GrossGrowth_BG-
    (tfi.DOMCO2Emission+tfi.DOMCH4Emssion+tfi.DOMCOEmission+
     tfi.DOMProduction + tfi.BiomassToSoil)) as [Delta Total DOM],
sum(iif(tfi.DistTypeID=0, tfi.GrossGrowth_AG+tfi.GrossGrowth_BG,0)) as [Net Primary Productivity (NPP)],
sum(iif(tfi.DistTypeID=0, tfi.GrossGrowth_AG+tfi.GrossGrowth_BG - 
    (tfi.DOMCO2Emission+tfi.DOMCH4Emssion+tfi.DOMCOEmission),0)) as [Net Ecosystem Productivity (NEP)],
sum(iif(tfi.DistTypeID=0,tfi.DeltaBiomass_AG+tfi.DeltaBiomass_BG,0)) as [Net Growth],
sum(iif(tfi.DistTypeID=0,tfi.DeltaDOM,0)) as [Net Litterfall],
sum(iif(tfi.DistTypeID=0,tfi.BiomassToSoil,0)) as [Total Litterfall],
sum(iif(tfi.DistTypeID=0, 
    tfi.VFastAGToAir+tfi.VFastBGToAir+tfi.FastAGToAir+tfi.FastBGToAir+
    tfi.MediumToAir+tfi.SlowAGToAir+tfi.SlowBGToAir+tfi.SWStemSnagToAir+
    tfi.SWBranchSnagToAir+tfi.HWStemSnagToAir+tfi.HWBranchSnagToAir+
    tfi.BlackCarbonToAir+tfi.PeatToAir, 0)) as [Decomposition Releases],
sum(tfi.SoftProduction+tfi.HardProduction+tfi.DOMProduction) as [Total Harvest (Biomass + Snags)],
sum(tfi.SoftProduction+tfi.HardProduction) as [Total Harvest (Biomass)],
sum(tfi.DOMProduction) as [Total Harvest (Snags)],
sum(tfi.SoftProduction) as [Softwood Harvest (Biomass)],
sum(tfi.HardProduction) as [Hardwood Harvest (Biomass)],




from tblFluxIndicators tfi
group by tfi.TimeStep
order by tfi.TimeStep