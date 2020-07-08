SELECT 
tblSPUDefault.SPUID,
tblAdminBoundaryDefault.AdminBoundaryName,
tblEcoBoundaryDefault.EcoBoundaryName
FROM tblEcoBoundaryDefault INNER JOIN (
	tblAdminBoundaryDefault INNER JOIN
		tblSPUDefault ON tblAdminBoundaryDefault.AdminBoundaryID =
			tblSPUDefault.AdminBoundaryID
) ON tblEcoBoundaryDefault.EcoBoundaryID = tblSPUDefault.EcoBoundaryID;
