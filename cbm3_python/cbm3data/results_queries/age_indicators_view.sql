SELECT 
tai.TimeStep,{0}
sum(tai.Area) as [Area],
sum(tai.Biomass) as [Biomass],
sum(tai.DOM) as [DOM],
sum(tai.AveAge * tai.Area)/sum(tai.Area) as [Average Age]
from tblAgeIndicators tai
group by tai.TimeStep{1}
order by tai.TimeStep{1}