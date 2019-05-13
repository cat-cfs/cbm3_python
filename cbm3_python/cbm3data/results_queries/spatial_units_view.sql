SELECT 
tblSPU.SPUID,
tblAdminBoundary.AdminBoundaryName as ProjectAdminBoundary,
tblEcoBoundary.EcoBoundaryName as ProjectEcoBoundary,
tblAdminBoundaryDefault.AdminBoundaryName as DefaultAdminBoundary,
tblEcoBoundaryDefault.EcoBoundaryName as DefaultEcoBoundary
FROM 
(
  tblEcoBoundaryDefault INNER JOIN (
    tblAdminBoundaryDefault INNER JOIN 
      tblSPUDefault ON 
      tblAdminBoundaryDefault.AdminBoundaryID = tblSPUDefault.AdminBoundaryID
    ) ON tblEcoBoundaryDefault.EcoBoundaryID = tblSPUDefault.EcoBoundaryID
) INNER JOIN (
  tblEcoBoundary INNER JOIN (
    tblAdminBoundary INNER JOIN tblSPU ON 
    tblAdminBoundary.AdminBoundaryID = tblSPU.AdminBoundaryID
  ) ON tblEcoBoundary.EcoBoundaryID = tblSPU.EcoBoundaryID
) ON tblSPUDefault.SPUID = tblSPU.DefaultSPUID;
