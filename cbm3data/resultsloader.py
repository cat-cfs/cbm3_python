# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import sys, os, shutil, math
from cbm3data.accessdb import AccessDB
from cbm3data.projectdb import ProjectDB
import logging
from math import floor

# a class to load cbm text file output that is too big for ms access mdb files
class ResultsLoader(object):
    
    
    def floored_percentage(self, val, digits):
        val *= 10 ** (digits + 2)
        return '{1:.{0}f}%'.format(digits, floor(val) / 10 ** digits)

    def file_len(self, fname):
        with open(fname) as f:
            for i, l in enumerate(f):
                pass
        return i + 1

    def loadResults(self,
                    outputDBPath,
                    aidbPath,
                    projectDBPath,
                    projectSimulationDirectory,
                    loadPreDistAge=False):

        with AccessDB(projectDBPath, False) as projectDB, \
             AccessDB(aidbPath, False) as aidb:

            self.copyAidbTables(aidb, outputDBPath)
            self.copyProjectTables(projectDB, outputDBPath)
            self.cset_dict = {}
            x = self.BuildCsetLookup(projectDB)

        with AccessDB(outputDBPath,False) as load_db:
            logging.info("creating indexes")
            self.createIndexes(load_db)

            self.addAnnualProcessDistType(load_db)
            self.loadAgeInd(projectSimulationDirectory, load_db)
            self.loadPoolInd(projectSimulationDirectory, load_db)
            self.loadDistInd(projectSimulationDirectory, load_db)
            self.loadFluxInd(projectSimulationDirectory, load_db)
            if loadPreDistAge:
                self.loadPreDistAge(projectSimulationDirectory, load_db)

        return outputDBPath

    def createIndexes(self, db):
        db.ExecuteQuery("CREATE INDEX fluxind_spuid_index ON tblFluxIndicators (SPUID)")
        db.ExecuteQuery("CREATE INDEX fluxind_kf3_index ON tblFluxIndicators (kf3)")
        db.ExecuteQuery("CREATE INDEX fluxind_kf6_index ON tblFluxIndicators (kf6)")
        db.ExecuteQuery("CREATE INDEX fluxind_TimeStep_index ON tblFluxIndicators (TimeStep)")
        db.ExecuteQuery("CREATE INDEX fluxind_UserDefdClassSetID_index ON tblFluxIndicators (UserDefdClassSetID)")
        db.ExecuteQuery("CREATE INDEX fluxind_landclass_index ON tblFluxIndicators (LandClassID)")
        

        db.ExecuteQuery("CREATE INDEX distind_spuid_index ON tblDistIndicators (SPUID)")
        db.ExecuteQuery("CREATE INDEX distind_kf3_index ON tblDistIndicators (kf3)")
        db.ExecuteQuery("CREATE INDEX distind_kf6_index ON tblDistIndicators (kf6)")
        db.ExecuteQuery("CREATE INDEX distind_TimeStep_index ON tblDistIndicators (TimeStep)")
        db.ExecuteQuery("CREATE INDEX distind_UserDefdClassSetID_index ON tblDistIndicators (UserDefdClassSetID)")
        db.ExecuteQuery("CREATE INDEX distind_landclass_index ON tblDistIndicators (LandClassID)")
        
        db.ExecuteQuery("CREATE UNIQUE INDEX distTypeID_index ON tblDisturbanceType (DistTypeID)")
        db.ExecuteQuery("CREATE UNIQUE INDEX spuid_index ON tblSPU (SPUID)")

    def addAnnualProcessDistType(self, rrdb):
        rrdb.ExecuteQuery("INSERT INTO tblDisturbanceType (DistTypeID, DistTypeName, DefaultDistTypeID) VALUES (0, 'unknown', 0)")

    def copyProjectTables(self, proj, rrdb_path):
        proj.ExecuteQuery("SELECT * INTO tblSPU IN '{0}' FROM tblSPU".format(rrdb_path))
        proj.ExecuteQuery("SELECT * INTO tblAdminBoundary IN '{0}' FROM tblAdminBoundary".format(rrdb_path))
        proj.ExecuteQuery("SELECT * INTO tblEcoBoundary IN '{0}' FROM tblEcoBoundary".format(rrdb_path))
        proj.ExecuteQuery("SELECT * INTO tblDisturbanceType IN '{0}' FROM tblDisturbanceType".format(rrdb_path))

    def copyAidbTables(self, aidb, rrdb_path):
        aidb.ExecuteQuery("SELECT * INTO tblSPUDefault IN '{0}' FROM tblSPUDefault".format(rrdb_path))
        aidb.ExecuteQuery("SELECT * INTO tblAdminBoundaryDefault IN '{0}' FROM tblAdminBoundaryDefault".format(rrdb_path))
        aidb.ExecuteQuery("SELECT * INTO tblEcoBoundaryDefault IN '{0}' FROM tblEcoBoundaryDefault".format(rrdb_path))
        aidb.ExecuteQuery("SELECT * INTO tblDisturbanceTypeDefault IN '{0}' FROM tblDisturbanceTypeDefault".format(rrdb_path))
    
    def BuildCsetLookup(self, projectDB):
        self.maxCsetId = 0
        csetQuery = """TRANSFORM First([qryClassifierSetValueNames].[ClassifierValueID]) AS FirstOfID
        SELECT [qryClassifierSetValueNames].[ClassifierSetID] AS ClassifierSetIDCroseTab
        FROM         
            (SELECT tblClassifierSetValues.ClassifierSetID, tblClassifierSetValues.ClassifierID, tblClassifierValues.Name, tblClassifierSetValues.ClassifierValueID
            FROM tblClassifierSetValues INNER JOIN tblClassifierValues ON tblClassifierSetValues.[ClassifierID] = tblClassifierValues.[ClassifierID]
            WHERE (((tblClassifierSetValues.ClassifierValueID)=[tblClassifierValues].[ClassifierValueID]))) as qryClassifierSetValueNames        
        GROUP BY [qryClassifierSetValueNames].[ClassifierSetID]
        PIVOT [qryClassifierSetValueNames].[ClassifierID];"""
        result = projectDB.Query(csetQuery)
        for row in result:
            cset_id = int(row[0])
            cset_string = ",".join([str(x) for x in row[1:]])
            if(cset_string in self.cset_dict):
                logging.info("duplicate cset {0} found in project".format(cset_string))
            if cset_id > self.maxCsetId:
                self.maxCsetId = cset_id
            self.cset_dict[cset_string] = cset_id
        return x
    
    def getClassifierSetID(self, classifierValueIds):
        # accept a list of integers representing a 
        # set of classifier value ids 
        # and return the matching classifier set id
        key = ",".join([str(x) for x in classifierValueIds])
        if key in self.cset_dict:
            return self.cset_dict[key]
        else:
            self.maxCsetId = self.maxCsetId + 1
            self.cset_dict[key] = self.maxCsetId
        return self.cset_dict[key]

    def mysplit(self, s, delim=None): # thanks to: http://stackoverflow.com/questions/2197451/why-are-empty-strings-returned-in-split-results
        return [x for x in s.split(delim) if x]
    
    def tokenize(self, infilePath, delim=None):
        f_input = open(infilePath)
        for line in f_input:
            yield self.mysplit(line, delim)

    def getAgeIndPath(self, cbmOutputDirectory):
        return os.path.join(cbmOutputDirectory, "ageind.out")

    def getDisturbIndPath(self, cbmOutputDirectory):
        return os.path.join(cbmOutputDirectory, "distinds.out")

    def getFluxIndPath(self, cbmOutputDirectory):
        return os.path.join(cbmOutputDirectory, "fluxind.out")
       
    def getPoolIndPath(self, cbmOutputDirectory):
        return os.path.join(cbmOutputDirectory, "poolind.out")

    def getPreDistAgePath(self, cbmOutputDirectory):
        return os.path.join(cbmOutputDirectory, "predistage.csv")

    def getCBMOutputDirectoy(self, projectSimulationDirectory):
        return os.path.join(projectSimulationDirectory, "CBMRun", "output")

    def loadAgeInd(self, projectSimulationDirectory, accessDB):
        simdir = self.getCBMOutputDirectoy(projectSimulationDirectory)
        ageIndPath = self.getAgeIndPath(simdir)

        qry = """
INSERT INTO tblAgeIndicators
(
    AgeIndID, TimeStep, SPUID, AgeClassID, UserDefdClassSetID, LandClassID, kf2, kf3, kf4, kf5, kf6, Area, Biomass, DOM, AveAge
) VALUES (
    {AgeIndID}, {TimeStep}, {SPUID}, {AgeClassID}, {UserDefdClassSetID}, {LandClassID}, {kf2}, {kf3}, {kf4}, {kf5}, {kf6}, {Area}, {Biomass}, {DOM}, {AveAge}
)
"""
        fileLen = self.file_len(ageIndPath);
        
        recordNum = 0
        for line in self.tokenize(ageIndPath):
            self.printProgress(fileLen, ageIndPath, recordNum)
            recordNum = recordNum + 1
            cset = [int(line[x]) for x in range(4,14) if int(line[x]) > 0]
            cset_id = self.getClassifierSetID(cset)

            qryFormatted = qry.format(AgeIndID = recordNum, 
                                      TimeStep = int(line[1]),
                                      SPUID = int(line[2]),
                                      AgeClassID = int(line[3]), 
                                      UserDefdClassSetID = int(cset_id),
                                      LandClassID = int(line[14]), 
                                      kf2 = int(line[15]),
                                      kf3 = int(line[16]),
                                      kf4 = int(line[17]),
                                      kf5 = int(line[18]),
                                      kf6 = int(line[19]),
                                      Area = float(line[20]),
                                      Biomass = float(line[21]),
                                      DOM = float(line[22]),
                                      AveAge = float(line[23]))

            accessDB.ExecuteQuery(qryFormatted)

    def loadDistInd(self, projectSimulationDirectory, accessDB):
        simdir = self.getCBMOutputDirectoy(projectSimulationDirectory)
        distIndPath = self.getDisturbIndPath(simdir)

        qry = """
INSERT INTO tblDistIndicators
(
 DistIndID, SPUID, DistTypeID, TimeStep, UserDefdClassSetID, 
 LandClassID, kf2, kf3, kf4, kf5, kf6, DistArea, DistProduct        
) VALUES (
 {DistIndID}, {SPUID}, {DistTypeID}, {TimeStep}, {UserDefdClassSetID}, 
 {LandClassID}, {kf2}, {kf3}, {kf4}, {kf5}, {kf6}, {DistArea}, {DistProduct}
)"""
        fileLen = self.file_len(distIndPath);
        
        recordNum = 0
        for line in self.tokenize(distIndPath):
            self.printProgress(fileLen, distIndPath, recordNum)
            recordNum = recordNum + 1

            cset = [int(line[x]) for x in range(4,14) if int(line[x]) > 0]
            cset_id = self.getClassifierSetID(cset)

            qryFormatted = qry.format(DistIndID=recordNum,
                                      SPUID = int(line[3]), DistTypeID = int(line[2]), 
                                      TimeStep = int(line[1]), UserDefdClassSetID = int(cset_id),
                                      LandClassID = int(line[14]), kf2= int(line[15]), kf3=int(line[16]),
                                      kf4=int(line[17]), kf5=int(line[18]), kf6=int(line[19]),
                                      DistArea = float(line[20]), DistProduct=float(line[21]))

            accessDB.ExecuteQuery(qryFormatted)

    def printProgress(self, fileLen, fluxIndPath, recordNum):
        if ((recordNum-1) % (fileLen/10)) == 0 or recordNum == fileLen:
            logging.info("load file {0}: {1}/{2} ({3})".format(
                fluxIndPath, recordNum, fileLen, 
                self.floored_percentage(float(recordNum)/fileLen, 2)))

    def loadFluxInd(self, projectSimulationDirectory, accessDB):
        simdir = self.getCBMOutputDirectoy(projectSimulationDirectory)
        fluxIndPath = self.getFluxIndPath(simdir)

        qry = """
INSERT INTO tblFluxIndicators
(
    FluxIndicatorID, TimeStep, 
    DistTypeID, SPUID,
    UserDefdClassSetID, 
    CO2Production, CH4Production, COProduction, 
    BioCO2Emission, BioCH4Emission, BioCOEmission, 
    DOMCO2Emission, DOMCH4Emssion, DOMCOEmission, 
    SoftProduction, HardProduction, DOMProduction, 
    DeltaBiomass_AG, DeltaBiomass_BG, DeltaDOM, 
    BiomassToSoil, MerchLitterInput, FolLitterInput, OthLitterInput, SubMerchLitterInput, CoarseLitterInput, FineLitterInput,
    VFastAGToAir, VFastBGToAir, FastAGToAir, FastBGToAir, MediumToAir, SlowAGToAir, SlowBGToAir, 
    SWStemSnagToAir, SWBranchSnagToAir, HWStemSnagToAir, HWBranchSnagToAir, 
    BlackCarbonToAir, PeatToAir, 
    LandClassID, kf2, kf3, kf4, kf5, kf6,
    MerchToAir, FolToAir, OthToAir, SubMerchToAir, CoarseToAir, FineToAir,
    GrossGrowth_AG, GrossGrowth_BG
)
VALUES
(
    {FluxIndicatorID}, {TimeStep},
    {DistTypeID}, {SPUID},
    {UserDefdClassSetID}, 
    {CO2Production}, {CH4Production}, {COProduction},
    {BioCO2Emission}, {BioCH4Emission}, {BioCOEmission},
    {DOMCO2Emission}, {DOMCH4Emssion}, {DOMCOEmission}, 
    {SoftProduction}, {HardProduction}, {DOMProduction}, 
    {DeltaBiomass_AG}, {DeltaBiomass_BG}, {DeltaDOM}, 
    {BiomassToSoil}, {MerchLitterInput}, {FolLitterInput}, {OthLitterInput}, {SubMerchLitterInput}, {CoarseLitterInput}, {FineLitterInput}, 
    {VFastAGToAir}, {VFastBGToAir}, {FastAGToAir}, {FastBGToAir}, {MediumToAir}, {SlowAGToAir}, {SlowBGToAir}, 
    {SWStemSnagToAir}, {SWBranchSnagToAir}, {HWStemSnagToAir}, {HWBranchSnagToAir}, 
    {BlackCarbonToAir}, {PeatToAir},
    {LandClassID}, {kf2}, {kf3}, {kf4}, {kf5}, {kf6}, 
    {MerchToAir}, {FolToAir}, {OthToAir}, {SubMerchToAir}, {CoarseToAir}, {FineToAir}, 
    {GrossGrowth_AG}, {GrossGrowth_BG}
)"""
        fileLen = self.file_len(fluxIndPath);
        
        recordNum = 0
        for line in self.tokenize(fluxIndPath):
            self.printProgress(fileLen, fluxIndPath, recordNum)
            recordNum = recordNum + 1

            cset = [int(line[x]) for x in range(4,14) if int(line[x]) > 0]
            cset_id = self.getClassifierSetID(cset)

            distTypeId = int(line[2])
            qryFormatted = qry.format(
                FluxIndicatorID = recordNum, TimeStep = int(line[1]), 
                DistTypeID = distTypeId, SPUID = int(line[3]),
                UserDefdClassSetID = int(cset_id), 
                CO2Production = float(line[20]), CH4Production = float(line[21]), COProduction = float(line[22]), 
                BioCO2Emission = float(line[23]), BioCH4Emission = float(line[24]), BioCOEmission = float(line[25]), 
                DOMCO2Emission = float(line[26]), DOMCH4Emssion = float(line[27]), DOMCOEmission = float(line[28]), 
                SoftProduction = float(line[29]), HardProduction = float(line[30]), DOMProduction = float(line[31]), 
                DeltaBiomass_AG = float(line[32]), DeltaBiomass_BG = float(line[33]), DeltaDOM = float(line[34]), 
                BiomassToSoil = float(line[35]), MerchLitterInput = float(line[36]), FolLitterInput = float(line[37]), OthLitterInput = float(line[38]),
                SubMerchLitterInput = float(line[39]), CoarseLitterInput = float(line[40]), FineLitterInput = float(line[41]),
                VFastAGToAir = float(line[42]), VFastBGToAir = float(line[43]), FastAGToAir = float(line[44]), FastBGToAir = float(line[45]),
                MediumToAir = float(line[46]), SlowAGToAir = float(line[47]), SlowBGToAir = float(line[48]), 
                SWStemSnagToAir = float(line[49]), SWBranchSnagToAir = float(line[50]), HWStemSnagToAir = float(line[51]), HWBranchSnagToAir = float(line[52]), 
                BlackCarbonToAir = float(line[53]), PeatToAir = float(line[54]), 
                LandClassID = float(line[14]), kf2 = float(line[15]), kf3 = float(line[16]), kf4 = float(line[17]), kf5 = float(line[18]), kf6 = float(line[19]),
                MerchToAir = float(line[55]), FolToAir = float(line[56]), OthToAir = float(line[57]), SubMerchToAir = float(line[58]), CoarseToAir = float(line[59]), FineToAir = float(line[60]),
                GrossGrowth_AG = float(line[32]) + float(line[36]) + float(line[37]) + float(line[38]) + float(line[39]) if distTypeId == 0 else 0, 
                GrossGrowth_BG = float(line[33]) + float(line[40]) + float(line[41]) if distTypeId == 0 else 0) 

            accessDB.ExecuteQuery(qryFormatted)

    def loadPoolInd(self, projectSimulationDirectory, accessDB):

        simdir = self.getCBMOutputDirectoy(projectSimulationDirectory)
        poolIndPath = self.getPoolIndPath(simdir)

        qry = """
INSERT INTO tblPoolIndicators
(
    PoolIndID,
    TimeStep, SPUID, UserDefdClassSetID,
    VFastAG, VFastBG, FastAG, FastBG, Medium, SlowAG, SlowBG, 
    SWStemSnag, SWBranchSnag, HWStemSnag, HWBranchSnag,
    BlackCarbon, Peat,
    LandClassID, kf2, kf3, kf4, kf5, kf6,
    SW_Merch, SW_Foliage, SW_Other, SW_subMerch, SW_Coarse, SW_Fine,
    HW_Merch, HW_Foliage, HW_Other, HW_subMerch, HW_Coarse, HW_Fine
)
VALUES
(
    {PoolIndID}, 
    {TimeStep}, {SPUID}, {UserDefdClassSetID}, 
    {VFastAG}, {VFastBG}, {FastAG}, {FastBG}, {Medium}, {SlowAG}, {SlowBG}, 
    {SWStemSnag}, {SWBranchSnag}, {HWStemSnag}, {HWBranchSnag}, 
    {BlackCarbon}, {Peat}, 
    {LandClassID}, {kf2}, {kf3}, {kf4}, {kf5}, {kf6}, 
    {SW_Merch}, {SW_Foliage}, {SW_Other}, {SW_subMerch}, {SW_Coarse}, {SW_Fine}, 
    {HW_Merch}, {HW_Foliage}, {HW_Other}, {HW_subMerch}, {HW_Coarse}, {HW_Fine}
)"""

        fileLen = self.file_len(poolIndPath);
        recordNum = 0
        for line in self.tokenize(poolIndPath):

            self.printProgress(fileLen, poolIndPath, recordNum)
            recordNum = recordNum + 1

            cset = [int(line[x]) for x in range(3,13) if int(line[x]) > 0]
            cset_id = self.getClassifierSetID(cset)
            
            
            qryFormatted = qry.format(PoolIndID=recordNum,
                      TimeStep = int(line[1]), 
                      SPUID = int(line[2]), 
                      UserDefdClassSetID = int(cset_id),
                      VFastAG = float(line[31]), VFastBG = float(line[32]), FastAG = float(line[33]), FastBG = float(line[34]), Medium = float(line[35]), SlowAG = float(line[36]), SlowBG = float(line[37]), 
                      SWStemSnag = float(line[38]), SWBranchSnag = float(line[39]), HWStemSnag =float(line[40]), HWBranchSnag = float(line[41]),
                      BlackCarbon = float(line[42]), Peat = float(line[43]),
                      LandClassID = float(line[13]), kf2 = float(line[14]), kf3 = float(line[15]), kf4 = float(line[16]), kf5 = float(line[17]), kf6 = float(line[18]),
                      SW_Merch = float(line[19]), SW_Foliage = float(line[20]), SW_Other = float(line[21]), SW_subMerch = float(line[22]), SW_Coarse = float(line[23]), SW_Fine = float(line[24]),
                      HW_Merch = float(line[25]), HW_Foliage = float(line[26]), HW_Other = float(line[27]), HW_subMerch = float(line[28]), HW_Coarse = float(line[29]), HW_Fine = float(line[30])  )

            accessDB.ExecuteQuery(qryFormatted)


    def loadPreDistAge(self, projectSimulationDirectory, accessDB):

        simdir = self.getCBMOutputDirectoy(projectSimulationDirectory)
        preDistAgePath = self.getPreDistAgePath(simdir)

        qry = """
INSERT INTO tblPreDistAge
(PreDistAgeID, SPUID, DistTypeID, TimeStep, UserDefdClassSetID, LandClassID, kf2, kf3, kf4, kf5, kf6, PreDisturbanceAge, AreaDisturbed)
VALUES
({PreDistAgeID}, {SPUID}, {DistTypeID}, {TimeStep}, {UserDefdClassSetID}, {LandClassID}, {kf2}, {kf3}, {kf4}, {kf5}, {kf6}, {PreDisturbanceAge}, {AreaDisturbed})"""

        fileLen = self.file_len(preDistAgePath);
        recordNum = 0
        firstLine = True
        for line in self.tokenize(preDistAgePath, ","):
            if firstLine:#skip header
                firstLine=False
                continue

            self.printProgress(fileLen, preDistAgePath, recordNum)
            recordNum = recordNum + 1

            cset = [int(line[x]) for x in range(3,13) if int(line[x]) > 0]
            cset_id = self.getClassifierSetID(cset)
            
            qryFormatted = qry.format(
                PreDistAgeID=recordNum,
                SPUID=int(line[0]),
                DistTypeID=int(line[1]),
                TimeStep=int(line[2]),
                UserDefdClassSetID=cset_id,
                LandClassID=int(line[13]),
                kf2=int(line[14]),
                kf3=int(line[15]),
                kf4=int(line[16]),
                kf5=int(line[17]),
                kf6=int(line[18]),
                PreDisturbanceAge=int(line[19]),
                AreaDisturbed=float(line[20]))

            accessDB.ExecuteQuery(qryFormatted)

