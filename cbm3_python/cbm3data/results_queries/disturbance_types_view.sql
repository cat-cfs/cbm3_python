SELECT
tblDisturbanceType.DistTypeID,
tblDisturbanceType.DefaultDistTypeID,
tblDisturbanceType.DistTypeName,
tblDisturbanceTypeDefault.DistTypeName as DefaultDistTypeName
FROM tblDisturbanceType 
INNER JOIN tblDisturbanceTypeDefault ON 
tblDisturbanceType.DefaultDistTypeID = tblDisturbanceTypeDefault.DistTypeID;