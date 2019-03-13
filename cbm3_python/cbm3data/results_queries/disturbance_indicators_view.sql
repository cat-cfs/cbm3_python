SELECT 
tdi.TimeStep,{0}
sum(tdi.DistArea) as [Area],
sum(tdi.DistProduct) as [Product]
from tblDistindicators tdi
group by tdi.TimeStep{1}
order by tdi.TimeStep{1}