SELECT 
tpi.TimeStep,{0}
sum(tpi.VFastAG) as [Aboveground Very Fast DOM],
sum(tpi.VFastBG) as [Belowground Very Fast DOM],
sum(tpi.FastAG) as [Aboveground Fast DOM],
sum(tpi.FastBG) as [Belowground Fast DOM],
sum(tpi.Medium) as [Medium DOM],
sum(tpi.SlowAG) as [Aboveground Slow DOM],
sum(tpi.SlowBG) as [Belowground Slow DOM],
sum(tpi.SWStemSnag) as [Softwood Stem Snag],
sum(tpi.SWBranchSnag) as [Softwood Branch Snag],
sum(tpi.HWStemSnag) as [Hardwood Stem Snag],
sum(tpi.HWBranchSnag) as [Hardwood Branch Snag],
sum(tpi.BlackCarbon) as BlackCarbon,
sum(tpi.Peat) as Peat,
sum(tpi.SW_Merch) as [Softwood Merchantable],
sum(tpi.SW_Foliage) as [Softwood Foliage],
sum(tpi.SW_Other) as [Softwood Other],
sum(tpi.SW_Coarse) as [Softwood Coarse Roots],
sum(tpi.SW_Fine) as [Softwood Fine Roots],
sum(tpi.HW_Merch) as [Hardwood Merchantable],
sum(tpi.HW_Foliage) as [Hardwood Foliage],
sum(tpi.HW_Other) as [Hardwood Other],
sum(tpi.HW_Coarse) as [Hardwood Coarse Roots],
sum(tpi.HW_Fine) as [Hardwood Fine Roots],
sum(tpi.SW_Merch+tpi.SW_Foliage+tpi.SW_Other+tpi.SW_Coarse
    +tpi.SW_Fine+tpi.HW_Merch+tpi.HW_Foliage+tpi.HW_Other 
    +tpi.HW_Coarse+tpi.HW_Fine) as [Total Biomass],
sum(tpi.SW_Merch+tpi.SW_Foliage+tpi.SW_Other+
    tpi.HW_Merch+tpi.HW_Foliage+tpi.HW_Other ) 
    as [Aboveground Biomass],
sum(tpi.SW_Coarse+tpi.SW_Fine+tpi.HW_Coarse+tpi.HW_Fine) 
    as [Belowground Biomass],
sum(tpi.VFastAG+tpi.VFastBG+tpi.FastAG+tpi.FastBG+tpi.Medium+tpi.SlowAG
    +tpi.SlowBG+tpi.SWStemSnag+tpi.SWBranchSnag+tpi.HWStemSnag
    +tpi.HWBranchSnag+tpi.BlackCarbon+ tpi.Peat+tpi.SW_Merch
    +tpi.SW_Foliage+tpi.SW_Other+tpi.SW_Coarse
    +tpi.SW_Fine+tpi.HW_Merch+tpi.HW_Foliage+tpi.HW_Other 
    +tpi.HW_Coarse+tpi.HW_Fine) as [Total Ecosystem],
sum(tpi.VFastAG+tpi.VFastBG+tpi.FastAG+tpi.FastBG+tpi.Medium+tpi.SlowAG
    +tpi.SlowBG+tpi.SWStemSnag+tpi.SWBranchSnag+tpi.HWStemSnag
    +tpi.HWBranchSnag+tpi.BlackCarbon+ tpi.Peat) as [Dead Organic Matter],
sum(tpi.VFastAG+tpi.FastAG+tpi.Medium+tpi.SlowAG
    +tpi.SWStemSnag+tpi.SWBranchSnag+tpi.HWStemSnag
    +tpi.HWBranchSnag) as [Aboveground DOM],
sum(tpi.VFastBG+tpi.FastBG+tpi.SlowBG) as [Belowground DOM],
sum(tpi.FastBG+tpi.Medium+tpi.SWStemSnag+tpi.SWBranchSnag
    +tpi.HWStemSnag+tpi.HWBranchSnag) as [Deadwood],
sum(tpi.VFastAG+tpi.FastAG+tpi.SlowAG) as [Litter],
sum(tpi.VFastBG+tpi.SlowBG+tpi.SlowBG+tpi.BlackCarbon) as [Soil C]
from tblPoolIndicators tpi
group by tpi.TimeStep{1}
order by tpi.TimeStep{1}