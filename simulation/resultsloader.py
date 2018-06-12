import sys
import os
import shutil
import math
import logging
import win32com.client
import csv
from cbm3data.accessdb import AccessDB
from math import floor

# A class to load cbm text file output that is too big for ms access mdb files.
class ResultsLoader(object):

    def floored_percentage(self, val, digits):
        val *= 10 ** (digits + 2)
        return '{1:.{0}f}%'.format(digits, floor(val) / 10 ** digits)

    def file_len(self, fname):
        with open(fname) as f:
            for i, l in enumerate(f):
                pass
        return i + 1

    def loadResults(self, outputDBPath, aidbPath, projectDBPath, projectSimulationDirectory, aggregateKf5=False, kf5RulesPath=False):

        if not os.path.exists(projectDBPath):
            raise ValueError("Specified projectDBPath '{0}' does not exist".format(projectDBPath))
        if not os.path.exists(aidbPath):
            raise ValueError("Specified aidbPath '{0}' does not exist".format(aidbPath))
        with AccessDB(projectDBPath) as projectDB, \
             AccessDB(aidbPath) as aidb, \
             AccessDB(outputDBPath, False) as out_db:
            self.copyProjectTables(projectDB, outputDBPath)
            self.copyAidbTables(aidb, outputDBPath)
            self.addAnnualProcessDistType(out_db)

            self.cset_dict = {}
            x = self.BuildCsetLookup(projectDB)

            logging.info("loading output files")
            self.loadOutputFiles(projectDB, projectSimulationDirectory, outputDBPath, aggregateKf5, kf5RulesPath)

            logging.info("creating indexes")
            self.createIndexes(out_db)

    def loadOutputFiles(self, projectDB, projectSimulationDirectory, dbPath, aggregateKf5, kf5RulesPath):
        for (fileName, table, cols, avgCols, scalarCols, lineProcessor) in (
            (
                "ageind.out",
                "tblAgeIndicators",
                ["TimeStep", "SPUID", "AgeClassID", "UserDefdClassSetID", "LandClassID",
                 "kf2", "kf3", "kf4", "kf5", "kf6", "Area", "Biomass", "DOM", "AveAge"],
                ["AveAge"],
                ["Area", "Biomass", "DOM"],
                self.ageIndLineProcessor
            ),
            (
                "distinds.out",
                "tblDistIndicators",
                ["SPUID", "DistTypeID", "TimeStep", "UserDefdClassSetID", "LandClassID",
                 "kf2", "kf3", "kf4", "kf5", "kf6", "DistArea", "DistProduct"],
                [],
                ["DistArea", "DistProduct"],
                self.distIndLineProcessor
            ),
            (
                "fluxind.out",
                "tblFluxIndicators",
                ["TimeStep", "DistTypeID", "SPUID", "UserDefdClassSetID", "CO2Production",
                 "CH4Production", "COProduction", "BioCO2Emission", "BioCH4Emission", "BioCOEmission",
                 "DOMCO2Emission", "DOMCH4Emssion", "DOMCOEmission", "SoftProduction", "HardProduction",
                 "DOMProduction", "DeltaBiomass_AG", "DeltaBiomass_BG", "DeltaDOM", "BiomassToSoil",
                 "MerchLitterInput", "FolLitterInput", "OthLitterInput", "SubMerchLitterInput",
                 "CoarseLitterInput", "FineLitterInput", "VFastAGToAir", "VFastBGToAir", "FastAGToAir",
                 "FastBGToAir", "MediumToAir", "SlowAGToAir", "SlowBGToAir", "SWStemSnagToAir",
                 "SWBranchSnagToAir", "HWStemSnagToAir", "HWBranchSnagToAir", "BlackCarbonToAir", "PeatToAir",
                 "LandClassID", "kf2", "kf3", "kf4", "kf5", "kf6", "MerchToAir", "FolToAir", "OthToAir",
                  "SubMerchToAir", "CoarseToAir", "FineToAir", "GrossGrowth_AG", "GrossGrowth_BG"],
                [],
                ["CO2Production", "CH4Production", "COProduction", "BioCO2Emission", "BioCH4Emission", "BioCOEmission", "DOMCO2Emission",
                 "DOMCH4Emssion", "DOMCOEmission", "SoftProduction", "HardProduction", "DOMProduction", "DeltaBiomass_AG", "DeltaBiomass_BG",
                 "DeltaDOM", "BiomassToSoil", "MerchLitterInput", "FolLitterInput", "OthLitterInput", "SubMerchLitterInput",
                 "CoarseLitterInput", "FineLitterInput", "VFastAGToAir", "VFastBGToAir", "FastAGToAir", "FastBGToAir", "MediumToAir",
                 "SlowAGToAir", "SlowBGToAir", "SWStemSnagToAir", "SWBranchSnagToAir", "HWStemSnagToAir", "HWBranchSnagToAir",
                 "BlackCarbonToAir", "PeatToAir", "MerchToAir", "FolToAir", "OthToAir", "SubMerchToAir", "CoarseToAir", "FineToAir",
                 "GrossGrowth_AG", "GrossGrowth_BG"],
                self.fluxIndLineProcessor
            ),
            (
                "poolind.out",
                "tblPoolIndicators",
                ["TimeStep", "SPUID", "UserDefdClassSetID", "VFastAG", "VFastBG", "FastAG",
                 "FastBG", "Medium", "SlowAG", "SlowBG", "SWStemSnag", "SWBranchSnag", "HWStemSnag",
                 "HWBranchSnag", "BlackCarbon", "Peat", "LandClassID", "kf2", "kf3", "kf4", "kf5", "kf6",
                 "SW_Merch", "SW_Foliage", "SW_Other", "SW_subMerch", "SW_Coarse", "SW_Fine", "HW_Merch",
                 "HW_Foliage", "HW_Other", "HW_subMerch", "HW_Coarse", "HW_Fine"],
                [],
                ["VFastAG", "VFastBG", "FastAG", "FastBG", "Medium", "SlowAG", "SlowBG", "SWStemSnag", "SWBranchSnag", "HWStemSnag",
                 "HWBranchSnag", "BlackCarbon", "Peat", "SW_Merch", "SW_Foliage", "SW_Other", "SW_subMerch", "SW_Coarse", "SW_Fine",
                 "HW_Merch", "HW_Foliage", "HW_Other", "HW_subMerch", "HW_Coarse", "HW_Fine"],
                self.poolIndLineProcessor
            )):
            
            resultsFilePath = self.getResultsFilePath(projectSimulationDirectory, fileName)
            if not os.path.exists(resultsFilePath):
                raise ValueError("Specified results file path {0} does not exist".format(resultsFilePath))
            self.load(projectDB, dbPath, table, cols, avgCols, scalarCols, resultsFilePath, lineProcessor, aggregateKf5, kf5RulesPath)
        
    def createIndexes(self, db):
        for query in ("CREATE INDEX fluxind_spuid_index ON tblFluxIndicators (SPUID)",
                      "CREATE INDEX fluxind_kf3_index ON tblFluxIndicators (kf3)",
                      "CREATE INDEX fluxind_kf6_index ON tblFluxIndicators (kf6)",
                      "CREATE INDEX fluxind_TimeStep_index ON tblFluxIndicators (TimeStep)",
                      "CREATE INDEX fluxind_UserDefdClassSetID_index ON tblFluxIndicators (UserDefdClassSetID)",
                      "CREATE INDEX fluxind_landclass_index ON tblFluxIndicators (LandClassID)",
                      "CREATE INDEX distind_spuid_index ON tblDistIndicators (SPUID)",
                      "CREATE INDEX distind_kf3_index ON tblDistIndicators (kf3)",
                      "CREATE INDEX distind_kf6_index ON tblDistIndicators (kf6)",
                      "CREATE INDEX distind_TimeStep_index ON tblDistIndicators (TimeStep)",
                      "CREATE INDEX distind_UserDefdClassSetID_index ON tblDistIndicators (UserDefdClassSetID)",
                      "CREATE INDEX distind_landclass_index ON tblDistIndicators (LandClassID)",
                      "CREATE UNIQUE INDEX distTypeID_index ON tblDisturbanceType (DistTypeID)",
                      "CREATE UNIQUE INDEX spuid_index ON tblSPU (SPUID)"):
            db.ExecuteQuery(query)

    def addAnnualProcessDistType(self, rrdb):
        rrdb.ExecuteQuery("INSERT INTO tblDisturbanceType (DistTypeID, DistTypeName, DefaultDistTypeID) VALUES (0, 'unknown', 0)")

    def copyProjectTables(self, proj, rrdb_path):
        for query in ("SELECT * INTO tblSPU IN '{0}' FROM tblSPU".format(rrdb_path),
                      "SELECT * INTO tblAdminBoundary IN '{0}' FROM tblAdminBoundary".format(rrdb_path),
                      "SELECT * INTO tblEcoBoundary IN '{0}' FROM tblEcoBoundary".format(rrdb_path),
                      "SELECT * INTO tblDisturbanceType IN '{0}' FROM tblDisturbanceType".format(rrdb_path)):
            proj.ExecuteQuery(query)

    def copyAidbTables(self, aidb, rrdb_path):
        for query in ("SELECT * INTO tblSPUDefault IN '{0}' FROM tblSPUDefault".format(rrdb_path),
                      "SELECT * INTO tblAdminBoundaryDefault IN '{0}' FROM tblAdminBoundaryDefault".format(rrdb_path),
                      "SELECT * INTO tblEcoBoundaryDefault IN '{0}' FROM tblEcoBoundaryDefault".format(rrdb_path),
                      "SELECT * INTO tblDisturbanceTypeDefault IN '{0}' FROM tblDisturbanceTypeDefault".format(rrdb_path)):
            aidb.ExecuteQuery(query)

    def BuildCsetLookup(self, projectDB):
        self.maxCsetId = 0
        csetQuery = \
            """
            TRANSFORM First([qryClassifierSetValueNames].[ClassifierValueID]) AS FirstOfID
            SELECT [qryClassifierSetValueNames].[ClassifierSetID] AS ClassifierSetIDCroseTab
            FROM (
                SELECT
                    tblClassifierSetValues.ClassifierSetID,
                    tblClassifierSetValues.ClassifierID,
                    tblClassifierValues.Name,
                    tblClassifierSetValues.ClassifierValueID
                FROM tblClassifierSetValues
                INNER JOIN tblClassifierValues
                    ON tblClassifierSetValues.[ClassifierID] = tblClassifierValues.[ClassifierID]
                WHERE tblClassifierSetValues.ClassifierValueID = [tblClassifierValues].[ClassifierValueID]) AS qryClassifierSetValueNames
            GROUP BY [qryClassifierSetValueNames].[ClassifierSetID]
            PIVOT [qryClassifierSetValueNames].[ClassifierID];
            """
            
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
        return [x for x in s.split(delim) if x is not None]

    def tokenize(self, infilePath):
        with open(infilePath) as f_input:
            for line in f_input:
                yield self.mysplit(line)

    def getResultsFilePath(self, projectSimulationDirectory, resultsFile):
        cbmOutputDir = os.path.join(projectSimulationDirectory, "CBMRun", "output")
        return os.path.join(cbmOutputDir, resultsFile)

    def printProgress(self, fileLen, filePath, recordNum):
        print "load file {0}: {1}/{2} ({3})".format(
            filePath, recordNum, fileLen,
            self.floored_percentage(float(recordNum) / fileLen, 2))

    def isSpace(self, value):
        if isinstance(value, str):
            return str(value).isspace() or str(value) == ""
        if isinstance(value, unicode):
            return unicode(value).isspace() or unicode(value) == ""
        return False

    def extractFields(self, recordSet, columnNames):
        fields = []
        for name in columnNames:
            try:
                fields.append(recordSet.Fields[name])
            except:
                raise IOError("No field named '{0}' found in input.".format(name))
        return fields
        
    def getKf5Categories(self, projectDB, kf5RulesPath):
        categories = dict(
            (int(row.disttypeid), {"defaultdisttypeid": int(row.defaultdisttypeid), "category": ""})
            for row in projectDB.Query("SELECT disttypeid, defaultdisttypeid FROM tbldisturbancetype"))
            
        with open(kf5RulesPath, "r") as kf5RulesFile:
            reader = csv.DictReader(kf5RulesFile)
            for line in reader:
                for distType, details in categories.iteritems():
                    if details["defaultdisttypeid"] == int(line["DefaultDistTypeID"]):
                        details["category"] = line["Code"]
        
        return categories
        
    def load(self, projectDB, dbPath, table, cols, avgCols, scalarCols, resultsFilePath, lineProcessor, aggregateKf5, kf5RulesPath):
        kf5Categories = self.getKf5Categories(projectDB, kf5RulesPath) if aggregateKf5 else None
        dbEngine = win32com.client.Dispatch("DAO.DBEngine.120")
        tempDBPath = ".".join((dbPath, "tmp"))
        shutil.copyfile(dbPath, tempDBPath)
        db = dbEngine.OpenDatabase(tempDBPath)
        try:
            fileLen = self.file_len(resultsFilePath);
            recordSet = db.OpenRecordset(table)
            fields = self.extractFields(recordSet, cols)
            for lineNum, line in enumerate(self.tokenize(resultsFilePath), 1):
                row = lineProcessor(line, kf5Categories)
                recordSet.AddNew()
                for i, value in enumerate(row):
                    if self.isSpace(value):
                        continue
                    fields[i].Value = value
                
                recordSet.Update()
                if lineNum % 10000 == 0:
                    self.printProgress(fileLen, resultsFilePath, lineNum)
            
            self.printProgress(fileLen, resultsFilePath, lineNum)
        finally:
            if recordSet:
                recordSet.Close()
            db.Close()
        
        with AccessDB(dbPath, False) as outputDB:
            timesteps = [row.ts for row in outputDB.Query("SELECT DISTINCT(timestep) AS ts FROM {} IN '{}'".format(table, tempDBPath))]
            for ts in timesteps:
                outputDB.ExecuteQuery("INSERT INTO {table} ({destCols}) SELECT {sourceCols} FROM {table} t IN '{temp_db}' WHERE timestep = {ts} GROUP BY {groupByCols}".format(
                    destCols=",".join(cols),
                    sourceCols=",".join(["SUM(t.{0}) AS {0}".format(col) if col in scalarCols
                                         else "SUM(t.{col} * (t.area / (SELECT SUM(area) FROM {table} IN '{temp_db}' WHERE timestep = {ts}))) AS {col}".format(
                                            col=col, table=table, temp_db=tempDBPath, ts=ts) if col in avgCols
                                         else col
                                         for col in cols]),
                    groupByCols=",".join(set(cols) - set(scalarCols) - set(avgCols)),
                    table=table,
                    temp_db=tempDBPath,
                    ts=ts))
                
            os.unlink(tempDBPath)

    def ageIndLineProcessor(self, lineData, kf5Categories=None):
        cset = [int(lineData[x]) for x in range(4, 14) if int(lineData[x]) > 0]
        cset_id = -1

        kf5 = int(lineData[18])
        kf3 = int(lineData[16])
        kf5Category = None
        if kf5Categories:
            parts = []
            if kf5 < 0:
                parts.append("u")
            parts.append(kf5Categories[abs(kf5)]["category"])
            parts.append("pre" if kf3 == 0 else "post")
            parts.append("1990")
            kf5Category = "_".join(parts)
            kf3 = None

        return [int(lineData[1]), int(lineData[2]), int(lineData[3]), int(cset_id), int(lineData[14]), int(lineData[15]),
                kf3, int(lineData[17]), kf5Category or kf5, int(lineData[19]), float(lineData[20]), float(lineData[21]),
                float(lineData[22]), float(lineData[23])]
                
    def distIndLineProcessor(self, lineData, kf5Categories=None):
        cset = [int(lineData[x]) for x in range(4, 14) if int(lineData[x]) > 0]
        cset_id = -1

        kf5 = int(lineData[18])
        kf3 = int(lineData[16])
        kf5Category = None
        if kf5Categories:
            parts = []
            if kf5 < 0:
                parts.append("u")
            parts.append(kf5Categories[abs(kf5)]["category"])
            parts.append("pre" if kf3 == 0 else "post")
            parts.append("1990")
            kf5Category = "_".join(parts)
            kf3 = None

        return [int(lineData[3]), int(lineData[2]), int(lineData[1]), int(cset_id), int(lineData[14]), int(lineData[15]),
                kf3, int(lineData[17]), kf5Category or kf5, int(lineData[19]), float(lineData[20]), float(lineData[21])]
        
    def fluxIndLineProcessor(self, lineData, kf5Categories=None):
        cset = [int(lineData[x]) for x in range(4, 14) if int(lineData[x]) > 0]
        cset_id = -1
        distTypeId = int(lineData[2])
        
        kf5 = int(lineData[18])
        kf3 = int(lineData[16])
        kf5Category = None
        if kf5Categories:
            parts = []
            if kf5 < 0:
                parts.append("u")
            parts.append(kf5Categories[abs(kf5)]["category"])
            parts.append("pre" if kf3 == 0 else "post")
            parts.append("1990")
            kf5Category = "_".join(parts)
            kf3 = None
    
        return [int(lineData[1]), int(lineData[2]), int(lineData[3]), int(cset_id), float(lineData[20]), float(lineData[21]),
                float(lineData[22]), float(lineData[23]), float(lineData[24]), float(lineData[25]), float(lineData[26]),
                float(lineData[27]), float(lineData[28]), float(lineData[29]), float(lineData[30]), float(lineData[31]),
                float(lineData[32]), float(lineData[33]), float(lineData[34]), float(lineData[35]), float(lineData[36]),
                float(lineData[37]), float(lineData[38]), float(lineData[39]), float(lineData[40]), float(lineData[41]),
                float(lineData[42]), float(lineData[43]), float(lineData[44]), float(lineData[45]), float(lineData[46]),
                float(lineData[47]), float(lineData[48]), float(lineData[49]), float(lineData[50]), float(lineData[51]),
                float(lineData[52]), float(lineData[53]), float(lineData[54]), float(lineData[14]), float(lineData[15]),
                kf3, float(lineData[17]), kf5Category or kf5, float(lineData[19]), float(lineData[55]), float(lineData[56]),
                float(lineData[57]), float(lineData[58]), float(lineData[59]), float(lineData[60]),
                float(lineData[32]) + float(lineData[36]) + float(lineData[37]) + float(lineData[38]) + float(lineData[39]) if distTypeId == 0 else 0,
                float(lineData[33]) + float(lineData[40]) + float(lineData[41]) if distTypeId == 0 else 0]
    
    def poolIndLineProcessor(self, lineData, kf5Categories=None):
        cset = [int(lineData[x]) for x in range(3, 13) if int(lineData[x]) > 0]
        cset_id = -1

        kf5 = int(lineData[17])
        kf3 = int(lineData[15])
        kf5Category = None
        if kf5Categories:
            parts = []
            if kf5 < 0:
                parts.append("u")
            parts.append(kf5Categories[abs(kf5)]["category"])
            parts.append("pre" if kf3 == 0 else "post")
            parts.append("1990")
            kf5Category = "_".join(parts)
            kf3 = None

        return [int(lineData[1]), int(lineData[2]), int(cset_id), float(lineData[31]), float(lineData[32]), float(lineData[33]),
                float(lineData[34]), float(lineData[35]), float(lineData[36]), float(lineData[37]), float(lineData[38]),
                float(lineData[39]), float(lineData[40]), float(lineData[41]), float(lineData[42]), float(lineData[43]),
                float(lineData[13]), float(lineData[14]), kf3, float(lineData[16]), kf5Category or kf5,
                float(lineData[18]), float(lineData[19]), float(lineData[20]), float(lineData[21]), float(lineData[22]),
                float(lineData[23]), float(lineData[24]), float(lineData[25]), float(lineData[26]), float(lineData[27]),
                float(lineData[28]), float(lineData[29]), float(lineData[30])]
    
