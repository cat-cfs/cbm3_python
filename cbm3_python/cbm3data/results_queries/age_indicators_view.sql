SELECT tai.TimeStep, tblAgeClasses.AgeRange,{0}
Sum(tai.Area) AS Area,
Sum(tai.Biomass*tai.Area)/Sum(tai.Area) AS [Average Biomass],
Sum(tai.DOM*tai.Area)/Sum(tai.Area) AS [Average DOM],
Sum(tai.AveAge*tai.Area)/Sum(tai.Area) AS [Average Age]
FROM tblAgeIndicators AS tai INNER JOIN tblAgeClasses
    ON tai.AgeClassID = tblAgeClasses.AgeClassID
GROUP BY tai.TimeStep, tblAgeClasses.AgeClassID, tblAgeClasses.AgeRange{1}
ORDER BY tai.TimeStep, tblAgeClasses.AgeClassID{1}
