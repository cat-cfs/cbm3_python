SELECT 
tpi.TimeStep,{0}
sum(tpi.VFastAG) as VFastAG,
sum(tpi.VFastBG) as VFastBG,
sum(tpi.FastAG) as FastAG,
sum(tpi.FastBG) as FastBG,
sum(tpi.Medium) as Medium,
sum(tpi.SlowAG) as SlowAG,
sum(tpi.SlowBG) as SlowBG,
sum(tpi.SWStemSnag) as SWStemSnag,
sum(tpi.SWBranchSnag) as SWBranchSnag,
sum(tpi.HWStemSnag) as HWStemSnag,
sum(tpi.HWBranchSnag) as HWBranchSnag,
sum(tpi.BlackCarbon) as BlackCarbon,
sum(tpi.Peat) as Peat,
sum(tpi.SW_Merch) as SW_Merch,
sum(tpi.SW_Foliage) as SW_Foliage,
sum(tpi.SW_Other) as SW_Other,
sum(tpi.SW_subMerch) as SW_subMerch,
sum(tpi.SW_Coarse) as SW_Coarse,
sum(tpi.SW_Fine) as SW_Fine,
sum(tpi.HW_Merch) as HW_Merch,
sum(tpi.HW_Foliage) as HW_Foliage,
sum(tpi.HW_Other) as HW_Other,
sum(tpi.HW_subMerch) as HW_subMerch,
sum(tpi.HW_Coarse) as HW_Coarse,
sum(tpi.HW_Fine) as HW_Fine,
sum(tpi.SW_Merch+tpi.SW_Foliage+tpi.SW_Other+tpi.SW_subMerch+tpi.SW_Coarse
    +tpi.SW_Fine+tpi.HW_Merch+tpi.HW_Foliage+tpi.HW_Other 
    +tpi.HW_subMerch+tpi.HW_Coarse+tpi.HW_Fine) as [Total Biomass],
sum(tpi.SW_Merch+tpi.SW_Foliage+tpi.SW_Other+tpi.SW_subMerch+ 
    tpi.HW_Merch+tpi.HW_Foliage+tpi.HW_Other +tpi.HW_subMerch) 
    as [Aboveground Biomass],
sum(tpi.SW_Coarse+tpi.SW_Fine+tpi.HW_Coarse+tpi.HW_Fine) 
    as [Belowground Biomass],
sum(tpi.VFastAG+tpi.VFastBG+tpi.FastAG+tpi.FastBG+tpi.Medium+tpi.SlowAG
    +tpi.SlowBG+tpi.SWStemSnag+tpi.SWBranchSnag+tpi.HWStemSnag
    +tpi.HWBranchSnag+tpi.BlackCarbon+ tpi.Peat+tpi.SW_Merch
    +tpi.SW_Foliage+tpi.SW_Other+tpi.SW_subMerch+tpi.SW_Coarse
    +tpi.SW_Fine+tpi.HW_Merch+tpi.HW_Foliage+tpi.HW_Other 
    +tpi.HW_subMerch+tpi.HW_Coarse+tpi.HW_Fine) as [Total Ecosystem],
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