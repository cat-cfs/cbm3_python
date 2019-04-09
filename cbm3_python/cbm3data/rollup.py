# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

# ---------------------------------------------------------------------------
#
#  Author: EN
#
#  Created on: June 11, 2014
#
#  Purpose: Encapsulate (right word?) the rollup queries in a class instead of carrying around bulky queries in the main script
#
#  Usage:
#  The class takes a few arguments, preferably a list or dictionary of the projects and results database paths.
#
#  Comments:
#
#   Tables to be added to:
#   1. AgeIndicators
#   2. DistIndicators
#   3. FluxIndicators
#   4. DistNotRealized
#   5. PoolIndicators
#   6. NIRSpecialOutput
#
#  Updated: History of changes made to the original script
#
#    Who When     What                                   Why
#    --- ----     ----                                   ----
#    EN           Started development, June 11, 2014     Why not!!??
#
# ---------------------------------------------------------------------------
import logging, os, shutil, subprocess, glob, string
from cbm3_python.cbm3data.accessdb import AccessDB

class Rollup(object):

    def __init__(self, run_results_database_paths, outputPath, aidbpath):
        self.RRDBPaths = run_results_database_paths
        self.OutputPath = outputPath
        self.AIDBPath = aidbpath

    def Roll(self):
        sqlMakeAgeInd = \
            """
            CREATE TABLE tblAgeIndicators (
            TimeStep Int,
            SPUID Int,
            AgeClassID Int,
            UserDefdClassSetID Int,
            LandClassID Int,
            kf2 Int,
            kf3 Int,
            kf4 Int,
            kf5 Int,
            kf6 Int,
            Area Single,
            Biomass Single,
            DOM Single,
            AveAge Single);
            """

        sqlMakeDistInd = """CREATE TABLE tblDistIndicators (
            SPUID Int,
            DistTypeID Int,
            TimeStep Int,
            UserDefdClassSetID Int,
            LandClassID Int,
            kf2 Int,
            kf3 Int,
            kf4 Int,
            kf5 Int,
            kf6 Int,
            DistArea Single,
            DistProduct Single);"""

        sqlMakeFluxInd = """CREATE TABLE tblFluxIndicators (
            TimeStep Int,
            DistTypeID Int,
            SPUID Int,
            UserDefdClassSetID Int,
            CO2Production Single,
            CH4Production Single,
            COProduction Single,
            BioCO2Emission Single,
            BioCH4Emission Single,
            BioCOEmission Single,
            DOMCO2Emission Single,
            DOMCH4Emssion Single,
            DOMCOEmission Single,
            SoftProduction Single,
            HardProduction Single,
            DOMProduction Single,
            DeltaBiomass_AG Single,
            DeltaBiomass_BG Single,
            DeltaDOM Single,
            BiomassToSoil Single,
            MerchLitterInput Single,
            FolLitterInput Single,
            OthLitterInput Single,
            SubMerchLitterInput Single,
            CoarseLitterInput Single,
            FineLitterInput Single,
            VFastAGToAir Single,
            VFastBGToAir Single,
            FastAGToAir Single,
            FastBGToAir Single,
            MediumToAir Single,
            SlowAGToAir Single,
            SlowBGToAir Single,
            SWStemSnagToAir Single,
            SWBranchSnagToAir Single,
            HWStemSnagToAir Single,
            HWBranchSnagToAir Single,
            BlackCarbonToAir Single,
            PeatToAir Single,
            LandClassID Int,
            kf2 Int,
            kf3 Int,
            kf4 Int,
            kf5 Int,
            kf6 Int,
            MerchToAir Single,
            FolToAir Single,
            OthToAir Single,
            SubMerchToAir Single,
            CoarseToAir Single,
            FineToAir Single,
            GrossGrowth_AG Single,
            GrossGrowth_BG Single);"""

        sqlMakeDistNotRealized = """CREATE TABLE tblDistNotRealized (
            TimeStep Int,
            DistTypeID Int,
            DistGroupID Int,
            UndistArea Single);"""

        sqlMakePoolInd = """CREATE TABLE tblPoolIndicators (
            TimeStep Int,
            SPUID Int,
            UserDefdClassSetID Int,
            VFastAG Single,
            VFastBG Single,
            FastAG Single,
            FastBG Single,
            Medium Single,
            SlowAG Single,
            SlowBG Single,
            SWStemSnag Single,
            SWBranchSnag Single,
            HWStemSnag Single,
            HWBranchSnag Single,
            BlackCarbon Single,
            Peat Single,
            LandClassID Int,
            kf2 Int, kf3 Int, kf4 Int, kf5 Int, kf6 Int,
            SW_Merch Single,
            SW_Foliage Single,
            SW_Other Single,
            SW_subMerch Single,
            SW_Coarse Single,
            SW_Fine Single,
            HW_Merch Single,
            HW_Foliage Single,
            HW_Other Single,
            HW_subMerch Single,
            HW_Coarse Single,
            HW_Fine Single);"""

        sqlMakePreDistAge = """
        CREATE TABLE tblPreDistAge (
        SPUID Int,
        DistTypeID Int,
        TimeStep Int,
        UserDefdClassSetID Int,
        LandClassID Int,
        kf2 Int,
        kf3 Int,
        kf4 Int,
        kf5 Int,
        kf6 Int,
        PreDisturbanceAge Int,
        AreaDisturbed Single
        );"""

        sqlAgeInd1 = """insert into tblAgeIndicators (
        TimeStep,
        SPUID,
        AgeClassID,
        UserDefdClassSetID,
        LandClassID,
        kf2,
        kf3,
        kf4,
        kf5,
        kf6,
        Area,
        Biomass,
        DOM,
        AveAge)
            SELECT
            ai.TimeStep,
            tblSPU.DefaultSPUID AS SPUID,
            ai.AgeClassID, -1 AS UserDefdClassSetID,
            ai.LandClassID,
            ai.kf2,
            ai.kf3,
            ai.kf4,
            IIF(Cint(ai.kf5)<0, -dt_kf5.DefaultDistTypeID, dt_kf5.DefaultDistTypeID) AS kf5,
            dt_kf6.DefaultDistTypeID AS kf6,
            Sum(ai.Area) AS Area,
            0 AS Biomass,
            0 AS DOM,
            0 AS AveAge
            FROM (
            tblSPU
            INNER JOIN (
                tblDisturbanceType AS dt_kf6
                INNER JOIN tblAgeIndicators AS ai ON dt_kf6.DistTypeID = ai.kf6
             ) ON tblSPU.SPUID = ai.SPUID
            ) INNER JOIN tblDisturbanceType AS dt_kf5 ON abs(Cint(ai.kf5)) = dt_kf5.DistTypeID
            IN '{0}'
            GROUP BY
            ai.TimeStep,
            tblSPU.DefaultSPUID,
            ai.AgeClassID,
            ai.LandClassID,
            ai.kf2,
            ai.kf3,
            ai.kf4,
            IIF(Cint(ai.kf5)<0, -dt_kf5.DefaultDistTypeID, dt_kf5.DefaultDistTypeID),
            dt_kf6.DefaultDistTypeID;"""


        sqlDistInd = """INSERT INTO tblDistIndicators (
            SPUID,
            DistTypeID,
            TimeStep,
            UserDefdClassSetID,
            LandClassID,
            kf2,
            kf3,
            kf4,
            kf5,
            kf6,
            DistArea,
            DistProduct)
            SELECT
                tblSPUDefault.SPUID,
                tblDisturbanceType.DefaultDistTypeID as DistTypeID,
                di.TimeStep,
                -1 AS UserDefdClassSetID,
                di.LandClassID,
                di.kf2,
                di.kf3,
                di.kf4,
                IIF(Cint(di.kf5) < 0, -dt_kf5.DefaultDistTypeID, dt_kf5.DefaultDistTypeID) as kf5,
                dt_kf6.DefaultDistTypeID AS kf6,
                Sum(di.DistArea) AS DistArea,
                Sum(di.DistProduct) AS DistProduct
                FROM (
                        (
                            (
                                tblSPUDefault INNER JOIN tblSPU ON tblSPUDefault.SPUID = tblSPU.DefaultSPUID
                            ) INNER JOIN (
                            tbldisturbancetype AS dt_kf6 INNER JOIN tblDistIndicators AS di ON dt_kf6.disttypeid = di.kf6
                            ) ON tblSPU.SPUID = di.SPUID
                        ) INNER JOIN tblDisturbanceType ON di.DistTypeID = tblDisturbanceType.DistTypeID
                     )
                INNER JOIN tblDisturbanceType AS dt_kf5 ON abs(Cint(di.kf5)) = dt_kf5.DistTypeID
                IN '{0}'
                GROUP BY
                tblSPUDefault.SPUID,
                tblDisturbanceType.DefaultDistTypeID,
                di.TimeStep,
                -1,
                di.LandClassID,
                di.kf2,
                di.kf3,
                di.kf4,
                IIF(Cint(di.kf5) < 0, -dt_kf5.DefaultDistTypeID, dt_kf5.DefaultDistTypeID),
                dt_kf6.DefaultDistTypeID;"""


        sqlFluxInd = """insert into tblFluxIndicators (TimeStep, DistTypeID, SPUID, UserDefdClassSetID, CO2Production,
                        CH4Production, COProduction, BioCO2Emission, BioCH4Emission, BioCOEmission, DOMCO2Emission,
                        DOMCH4Emssion, DOMCOEmission, SoftProduction, HardProduction, DOMProduction, DeltaBiomass_AG,
                        DeltaBiomass_BG, DeltaDOM, BiomassToSoil, MerchLitterInput, FolLitterInput, OthLitterInput,
                        SubMerchLitterInput, CoarseLitterInput, FineLitterInput, VFastAGToAir, VFastBGToAir, FastAGToAir,
                        FastBGToAir, MediumToAir, SlowAGToAir, SlowBGToAir, SWStemSnagToAir, SWBranchSnagToAir,
                        HWStemSnagToAir, HWBranchSnagToAir, BlackCarbonToAir, PeatToAir, LandClassID, kf2, kf3, kf4,
                        kf5, kf6, MerchToAir, FolToAir, OthToAir, SubMerchToAir, CoarseToAir, FineToAir, GrossGrowth_AG, GrossGrowth_BG)
               SELECT
                    FI.TimeStep,
                    tblDisturbanceType.DefaultDistTypeID,
                    tblSPU.defaultSPUID, -1 AS UserDefdClassSetID,
                    Sum(FI.CO2Production) AS CO2Production, Sum(FI.CH4Production) AS CH4Production,
                    Sum(FI.COProduction) AS COProduction, Sum(FI.BioCO2Emission) AS BioCO2Emission,
                    Sum(FI.BioCH4Emission) AS BioCH4Emission, Sum(FI.BioCOEmission) AS BioCOEmission,
                    Sum(FI.DOMCO2Emission) AS DOMCO2Emission, Sum(FI.DOMCH4Emssion) AS DOMCH4Emssion,
                    Sum(FI.DOMCOEmission) AS DOMCOEmission, Sum(FI.SoftProduction) AS SoftProduction,
                    Sum(FI.HardProduction) AS HardProduction, Sum(FI.DOMProduction) AS DOMProduction,
                    Sum(FI.DeltaBiomass_AG) AS DeltaBiomass_AG, Sum(FI.DeltaBiomass_BG) AS DeltaBiomass_BG,
                    Sum(FI.DeltaDOM) AS DeltaDOM, Sum(FI.BiomassToSoil) AS BiomassToSoil,
                    Sum(FI.MerchLitterInput) AS MerchLitterInput, Sum(FI.FolLitterInput) AS FolLitterInput,
                    Sum(FI.OthLitterInput) AS OthLitterInput, Sum(FI.SubMerchLitterInput) AS SubMerchLitterInput,
                    Sum(FI.CoarseLitterInput) AS CoarseLitterInput, Sum(FI.FineLitterInput) AS FineLitterInput,
                    Sum(FI.VFastAGToAir) AS VFastAGToAir, Sum(FI.VFastBGToAir) AS VFastBGToAir,
                    Sum(FI.FastAGToAir) AS FastAGToAir, Sum(FI.FastBGToAir) AS FastBGToAir,
                    Sum(FI.MediumToAir) AS MediumToAir, Sum(FI.SlowAGToAir) AS SlowAGToAir,
                    Sum(FI.SlowBGToAir) AS SlowBGToAir, Sum(FI.SWStemSnagToAir) AS SWStemSnagToAir,
                    Sum(FI.SWBranchSnagToAir) AS SWBranchSnagToAir, Sum(FI.HWStemSnagToAir) AS HWStemSnagToAir,
                    Sum(FI.HWBranchSnagToAir) AS HWBranchSnagToAir, Sum(FI.BlackCarbonToAir) AS BlackCarbonToAir,
                    Sum(FI.PeatToAir) AS PeatToAir,
                    FI.LandClassID,
                    FI.kf2,
                    FI.kf3,
                    FI.kf4,
                    IIF(Cint(FI.kf5)<0,-dt_kf5.DefaultDistTypeID,dt_kf5.DefaultDistTypeID) as kf5,
                    dt_kf6.DefaultDistTypeID AS kf6,
                    Sum(FI.MerchToAir) AS MerchToAir, Sum(FI.FolToAir) AS FolToAir, Sum(FI.OthToAir) AS OthToAir,
                    Sum(FI.SubMerchToAir) AS SubMerchToAir, Sum(FI.CoarseToAir) AS CoarseToAir,
                    Sum(FI.FineToAir) AS FineToAir, Sum(FI.GrossGrowth_AG) AS GrossGrowth_AG,
                    Sum(FI.GrossGrowth_BG) AS GrossGrowth_BG
                    FROM (
                        tblSPU INNER JOIN (
                            (
                                tblDisturbanceType INNER JOIN tblFluxIndicators AS FI ON tblDisturbanceType.DistTypeID = FI.DistTypeID
                            ) INNER JOIN tblDisturbanceType AS dt_kf6 ON FI.Kf6 = dt_kf6.DistTypeID
                        ) ON tblSPU.SPUID = FI.SPUID
                    ) INNER JOIN tblDisturbanceType AS dt_kf5 ON abs(Cint(FI.kf5)) = dt_kf5.DistTypeID
                    IN '{0}'
                    GROUP BY
                    FI.TimeStep,
                    tblDisturbanceType.DefaultDistTypeID,
                    tblSPU.defaultSPUID,
                    FI.LandClassID,
                    FI.kf2,
                    FI.kf3,
                    FI.kf4,
                    IIF(Cint(FI.kf5)<0,-dt_kf5.DefaultDistTypeID,dt_kf5.DefaultDistTypeID),
                    dt_kf6.DefaultDistTypeID;"""


        sqlPoolInd = """insert into tblPoolIndicators (
                        TimeStep, SPUID, UserDefdClassSetID, VFastAG, VFastBG, FastAG, FastBG,
                        Medium, SlowAG, SlowBG, SWStemSnag, SWBranchSnag, HWStemSnag, HWBranchSnag,
                        BlackCarbon, Peat, LandClassID, kf2, kf3, kf4, kf5, kf6, SW_Merch, SW_Foliage,
                        SW_Other, SW_subMerch, SW_Coarse, SW_Fine, HW_Merch, HW_Foliage, HW_Other,
                        HW_subMerch, HW_Coarse, HW_Fine
                    )
                    SELECT
                    PI.TimeStep,
                    tblSPU.defaultSPUID,
                    -1 AS UserDefdClassSetID,
                    Sum(PI.VFastAG) AS VFastAG, Sum(PI.VFastBG) AS VFastBG, Sum(PI.FastAG) AS FastAG,
                    Sum(PI.FastBG) AS FastBG, Sum(PI.Medium) AS Medium, Sum(PI.SlowAG) AS SlowAG,
                    Sum(PI.SlowBG) AS SlowBG, Sum(PI.SWStemSnag) AS SWStemSnag,
                    Sum(PI.SWBranchSnag) AS SWBranchSnag, Sum(PI.HWStemSnag) AS HWStemSnag,
                    Sum(PI.HWBranchSnag) AS HWBranchSnag, Sum(PI.BlackCarbon) AS BlackCarbon,
                    Sum(PI.Peat) AS Peat,
                    PI.LandClassID,
                    PI.kf2,
                    PI.kf3,
                    PI.kf4,
                    IIF(Cint(PI.kf5) < 0, -dt_kf5.DefaultDistTypeID, dt_kf5.DefaultDistTypeID) as kf5,
                    dt_kf6.DefaultDistTypeID AS kf6,
                    Sum(PI.SW_Merch) AS SW_Merch, Sum(PI.SW_Foliage) AS SW_Foliage, Sum(PI.SW_Other) AS SW_Other,
                    Sum(PI.SW_subMerch) AS SW_subMerch, Sum(PI.SW_Coarse) AS SW_Coarse, Sum(PI.SW_Fine) AS SW_Fine,
                    Sum(PI.HW_Merch) AS HW_Merch, Sum(PI.HW_Foliage) AS HW_Foliage, Sum(PI.HW_Other) AS HW_Other,
                    Sum(PI.HW_subMerch) AS HW_subMerch, Sum(PI.HW_Coarse) AS HW_Coarse, Sum(PI.HW_Fine) AS HW_Fine
                    FROM (
                            (
                                tblPoolIndicators AS PI INNER JOIN tblDisturbanceType as dt_kf6 ON PI.kf6 = dt_kf6.DistTypeID
                            ) INNER JOIN tblSPU ON PI.SPUID = tblSPU.SPUID
                        ) INNER JOIN tblDisturbanceType AS dt_kf5 ON abs(Cint(PI.kf5)) = dt_kf5.DistTypeID
                    IN '{0}'
                    GROUP BY
                    PI.TimeStep,
                    tblSPU.defaultSPUID,
                    PI.LandClassID,
                    PI.kf2,
                    PI.kf3,
                    PI.kf4,
                    IIF(Cint(PI.kf5) < 0, -dt_kf5.DefaultDistTypeID, dt_kf5.DefaultDistTypeID),
                    dt_kf6.DefaultDistTypeID;"""


        sqlDistNotRealized = """insert into tblDistNotRealized (TimeStep, DistTypeID, DistGroupID, UndistArea)
            SELECT TimeStep, DefaultDistTypeID, DefaultSPUID, sum(tDNR.UndistArea) as UndistArea
            FROM tblDisturbanceType
                INNER JOIN (
                    tblDistNotRealized tDNR
                        INNER JOIN (
                            tblSPU
                                INNER JOIN (
                                    tblSPUGroup
                                        INNER JOIN
                                            tblSPUGroupLookup ON tblSPUGroup.SPUGroupID = tblSPUGroupLookup.SPUGroupID
                                            ) ON tblSPU.SPUID = tblSPUGroupLookup.SPUID
                                    ) ON tDNR.DistGrouopID = tblSPUGroup.SPUGroupID
                            ) ON tblDisturbanceType.DistTypeID = tDNR.DistTypeID
            in '{0}'
            where tDNR.UnDistArea <> 0
            group by TimeStep, DefaultDistTypeID, tblSPU.DefaultSPUID"""


        sql_predistage = """
        insert into tblPreDistAge (SPUID, DistTypeID, TimeStep, UserDefdClassSetID, LandClassID, kf2, kf3, kf4, kf5, kf6, PreDisturbanceAge, AreaDisturbed)
            SELECT
            tblSPU.DefaultSPUID,
            tblDisturbanceType.DefaultDistTypeID,
            tblPreDistAge.TimeStep,
            -1 as UserDefdClassSetID,
            tblPreDistAge.LandClassID,
            tblPreDistAge.kf2,
            tblPreDistAge.kf3,
            tblPreDistAge.kf4,
            IIF(Cint(tblPreDistAge.kf5)<0, -dt_kf5.DefaultDistTypeID, dt_kf5.DefaultDistTypeID) AS kf5,
            dt_kf6.DefaultDistTypeID AS kf6,
            tblPreDistAge.PreDisturbanceAge,
            Sum(tblPreDistAge.AreaDisturbed) AS AreaDisturbed
            FROM (tblSPU INNER JOIN
            (
                (
                    (tblPreDistAge INNER JOIN tblDisturbanceType AS dt_kf5 ON abs(Cint(tblPreDistAge.kf5)) = dt_kf5.DistTypeID)
                    INNER JOIN tblDisturbanceType AS dt_kf6 ON tblPreDistAge.kf6 = dt_kf6.DistTypeID
                )
                INNER JOIN tblDisturbanceType ON tblPreDistAge.DistTypeID = tblDisturbanceType.DistTypeID
                )
               ON tblSPU.SPUID = tblPreDistAge.SPUID
            )
            in '{0}'
            GROUP BY
            tblSPU.DefaultSPUID,
            tblDisturbanceType.DefaultDistTypeID,
            tblPreDistAge.TimeStep,
            -1,
            tblPreDistAge.LandClassID,
            tblPreDistAge.kf2,
            tblPreDistAge.kf3,
            tblPreDistAge.kf4,
            IIF(Cint(tblPreDistAge.kf5)<0, -dt_kf5.DefaultDistTypeID, dt_kf5.DefaultDistTypeID),
            dt_kf6.DefaultDistTypeID,
            tblPreDistAge.PreDisturbanceAge;"""



        #
        #   Copy an empty DB to house the Net Net Results.
        #

        #   Need more tables for rollup
        #
        #   We need tblDisturbanceTypeDefault, tblSPUDefault, tblEcoboundaryDefault
        #
        sqlDistTypeDefault = """Select * into tblDisturbanceTypeDefault from tblDisturbanceTypeDefault in '{0}' """.format(self.AIDBPath)
        sqlSPUDefault = """Select *  into tblSPUDefault from tblSPUDefault in '{0}' """.format(self.AIDBPath)
        sqlEcoDefault = """Select * into tblEcoboundaryDefault from tblEcoboundaryDefault in '{0}' """.format(self.AIDBPath)
        sqlAdminDefault = """Select * into tblAdminBoundaryDefault from tblAdminBoundaryDefault in '{0}' """.format(self.AIDBPath)


        #
        #  The meat of the Class:
        #


        logging.info('Rolling up....')
        with AccessDB(self.OutputPath, True) as rollup:
            rollup.ExecuteQuery(sqlDistTypeDefault)
            rollup.ExecuteQuery(sqlSPUDefault)
            rollup.ExecuteQuery(sqlEcoDefault)
            rollup.ExecuteQuery(sqlAdminDefault)
            rollup.ExecuteQuery(sqlMakePoolInd)
            rollup.ExecuteQuery(sqlMakeAgeInd)
            rollup.ExecuteQuery(sqlMakeDistInd)
            rollup.ExecuteQuery(sqlMakeFluxInd)
            rollup.ExecuteQuery(sqlMakeDistNotRealized)
            rollup.ExecuteQuery(sqlMakePreDistAge)
            for project in self.RRDBPaths:

                sqlAge = sqlAgeInd1.format(project)
                logging.info("running age indicators query on project {0}".format(project))
                rollup.ExecuteQuery(sqlAge)

                sqlDist = sqlDistInd.format(project)
                logging.info("running dist indicators query on project {0}".format(project))
                rollup.ExecuteQuery(sqlDist)


                sqlFlux = sqlFluxInd.format(project)
                logging.info("running flux indicators query on project {0}".format(project))
                rollup.ExecuteQuery(sqlFlux)


                sqlDistNot = sqlDistNotRealized.format(project)
               # logging.info("running unrealized disturbances query on project {0}".format(project))
               # rollup.ExecuteQuery(sqlDistNot)


                sqlPoolIndInc = sqlPoolInd.format(project)
                logging.info("running pool indicators query on project {0}".format(project))
                rollup.ExecuteQuery(sqlPoolIndInc)

                logging.info("running pre dist ages query on project {0}".format(project))
                rollup.ExecuteQuery(sql_predistage.format(project))

            logging.info('I worked')

            return rollup.path




