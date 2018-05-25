import pyodbc
import logging
import os

from accessdb import AccessDB

# Scott - Nov 2013
# wrapper for ms access archive index database
# allows adding and deleting projects from the archive index
class AIDB(AccessDB):


    def InsertRowTo_tblInputDB(self, dir, name, inputDBID):
        return self.ExecuteQuery("INSERT INTO tblInputDB (InputDBID, Name, Description, Path, InputPermArchID)" +
                                 "VALUES(?, ?, ?, ?, 1)",
                                 (inputDBID, name, "", dir))

    
    def InsertRowTo_tblStandInitialization(self, name, desc, author, inputDBID, 
                                           standInitializationID, inputStandInitializationID):
        return self.ExecuteQuery(
               """
               INSERT INTO tblStandInitialization (
                   StandInitializationID,
                   Name,
                   Description,
                   Author,
                   Status,
                   ClientID,
                   InputDBID,
                   InputStandInitializationID,
                   MakelistVersionID
               )
               VALUES (?, ?, ?, ?, 2, 0, ?, ?, 1);
               """,(standInitializationID, name, desc, author, inputDBID, inputStandInitializationID))


    def InsertRowTo_tblCBMRun(self, name, desc, author, inputDBID, cbmRunID, inputCBMRunID):
        self.ExecuteQuery(
        """
        INSERT INTO tblCBMRun (
            CBMRunID,
            Name,
            Description,
            Author,
            Status,
            ClientID,
            InputDBID,
            InputCBMRunID,
            CBMVersionID
        )
        VALUES (?, ?, ?, ?, 2, 0, ?, ?, 1);
        """,(cbmRunID, name, desc, author, inputDBID, inputCBMRunID))


    def InsertRowTo_tblSimulation(self, name, desc, author, inputDBID, simulationID, standInitializationID, CBMRunID, inputSimulationID):
        self.ExecuteQuery(
        """
        INSERT INTO tblSimulation (
            SimulationID,
            Name,
            Description,
            Author,
            Status,
            StandInitializationID,
            CBMRunID,
            InputSimulationID,
            InputDBID,
            ResultsPermArchID,
            RulesVersionID,
            YearsInTimeStep
        )
        VALUES (
            ?,?,?,?,2,?,?,?,?,1,4,1
        )
        """,(simulationID, name, desc, author, standInitializationID, CBMRunID, inputSimulationID, inputDBID))


    def AddProjectToAIDB(self, project, simId=None):
        '''
        add a project to the AIDB with the next available SimulationID
        returns the simulation ID
        '''
        name_tokens = os.path.split(project.path)
        name = name_tokens[len(name_tokens) - 1]
        dir = os.path.dirname(project.path)

        nextInputDBID = self.GetMaxID("tblInputDB", "InputDBID") + 1
        nextStandInitializationID = self.GetMaxID("tblStandInitialization", "StandInitializationID") + 1
        nextCBMRunID = self.GetMaxID("tblCBMRun", "CBMRunID") + 1
        if not simId is None:
            nextSimulationID = simId
        else:
            nextSimulationID = self.GetMaxID("tblSimulation", "SimulationID") + 1

        self.InsertRowTo_tblInputDB(dir, name, nextInputDBID)

        ProjectToAIDBStandInitIDs = {}
        for standInitRow in project.Query("SELECT * FROM tblStandInitialization").fetchall():
            self.InsertRowTo_tblStandInitialization( 
                standInitRow.Name, standInitRow.Description, " ", 
                nextInputDBID, nextStandInitializationID, standInitRow.StandInitID)
            ProjectToAIDBStandInitIDs[standInitRow.StandInitID] = nextStandInitializationID
            nextStandInitializationID += 1
            

        RunIDToCBMRunIDLookup = {}
        for runRow in project.Query("SELECT * FROM tblRunTable").fetchall():
            self.InsertRowTo_tblCBMRun(runRow.Name, runRow.Description, " ", nextInputDBID, nextCBMRunID, runRow.RunID)
            RunIDToCBMRunIDLookup[runRow.RunID] = nextCBMRunID
            nextCBMRunID += 1

        for simRow in project.Query("SELECT * FROM tblSimulation").fetchall():
            self.InsertRowTo_tblSimulation(simRow.Name, simRow.Description, " ", nextInputDBID, nextSimulationID, 
                                           ProjectToAIDBStandInitIDs[simRow.StandInitID], RunIDToCBMRunIDLookup[simRow.RunID],
                                           simRow.SimulationID)
            nextSimulationID += 1

        return nextSimulationID - 1


    def DeleteProjectsFromAIDB(self, simulation_id = None):
        '''
        remove a project associated with the given simulation id from the AIDB
        if no simulation id is given then the aidb will be cleared of all projects  
        '''
        if simulation_id is None:
            self.ExecuteQuery("DELETE FROM tblInputDB")
            self.ExecuteQuery("DELETE FROM tblSimulation")
            self.ExecuteQuery("DELETE FROM tblStandInitialization")
            self.ExecuteQuery("DELETE FROM tblCBMRun")
        keys = self.getKeys(simulation_id)
        for item in keys:
            self.ExecuteQuery("DELETE FROM tblInputDB WHERE InputDBID = ?",
                         (item.InputDBID))


    def getKeys(self, simulation_id = None):

        filterString = None
        if simulation_id == None:
            filterString = ";"
        else:
            filterString = "WHERE tblSimulation.SimulationID = {0};".format(int(simulation_id))
        keys = self.Query(
        """
        SELECT 
        tblSimulation.SimulationID, 
        tblInputDB.InputDBID, 
        tblCBMRun.CBMRunID, 
        tblStandInitialization.StandInitializationID
        FROM tblStandInitialization 
        INNER JOIN 
        (
            tblCBMRun INNER JOIN 
            ( 
                tblInputDB INNER JOIN tblSimulation ON tblInputDB.InputDBID = tblSimulation.InputDBID
            ) 
            ON tblCBMRun.CBMRunID = tblSimulation.CBMRunID
        )
        ON tblStandInitialization.StandInitializationID = tblSimulation.StandInitializationID
        {0}
        """.format(filterString)).fetchall()

        return keys
