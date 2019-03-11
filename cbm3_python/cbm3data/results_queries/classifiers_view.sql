SELECT
tblUserDefdClassSetValues.UserDefdClassSetID,
tblUserDefdClasses.ClassDesc,
tblUserDefdSubclasses.UserDefdSubClassName
FROM ( 
    tblUserDefdClasses INNER JOIN tblUserDefdClassSetValues ON
    tblUserDefdClasses.UserDefdClassID = tblUserDefdClassSetValues.UserDefdClassID
) INNER JOIN tblUserDefdSubclasses ON (
    tblUserDefdClassSetValues.UserDefdSubclassID = tblUserDefdSubclasses.UserDefdSubclassID
) AND (
    tblUserDefdClassSetValues.UserDefdClassID = tblUserDefdSubclasses.UserDefdClassID)
ORDER BY  tblUserDefdClassSetValues.UserDefdClassSetID, tblUserDefdClasses.UserDefdClassID