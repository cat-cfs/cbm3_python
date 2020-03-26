SELECT tai.TimeStep, tblAgeClasses.AgeClassID, tblAgeClasses.AgeRange,{0}
tai.Area AS Area,
tai.Biomass AS Biomass,
tai.DOM AS DOM,
tai.AveAge AS AveAge
FROM tblAgeIndicators AS tai INNER JOIN tblAgeClasses
    ON tai.AgeClassID = tblAgeClasses.AgeClassID
ORDER BY tai.TimeStep, tblAgeClasses.AgeClassID{1}
