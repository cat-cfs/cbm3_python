# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import os

from cbm3_python.cbm3data.accessdb import AccessDB

# Scott - Nov 2013
# wrapper for ms access archive index database
# allows adding and deleting projects from the archive index


class AIDB(AccessDB):

    def InsertRowTo_tblInputDB(self, dir, name, inputDBID):
        return self.ExecuteQuery(
            "INSERT INTO tblInputDB " +
            "(InputDBID, Name, Description, Path, InputPermArchID)" +
            "VALUES(?, ?, ?, ?, 1)",
            (inputDBID, name, "", dir))

    def InsertRowTo_tblStandInitialization(self, name, desc, author, inputDBID,
                                           standInitializationID,
                                           inputStandInitializationID):
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
            """, (standInitializationID, name, desc, author, inputDBID,
                  inputStandInitializationID))

    def InsertRowTo_tblCBMRun(self, name, desc, author, inputDBID, cbmRunID,
                              inputCBMRunID):
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
            """, (cbmRunID, name, desc, author, inputDBID, inputCBMRunID))

    def InsertRowTo_tblSimulation(self, name, desc, author, inputDBID,
                                  simulationID, standInitializationID,
                                  CBMRunID, inputSimulationID):
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
            """, (simulationID, name, desc, author, standInitializationID,
                  CBMRunID, inputSimulationID, inputDBID))

    def AddProjectToAIDB(self, project, project_sim_id=None):
        """Add a single scenario form the specified project to the Archive
        index database.  If the project_sim_id is specified, the simulation
        scenario id matching that id will be added to the archive index, and
        otherwise, the simulation scenario with the highest id value will be
        added.

        Args:
            project (cbm3_python.cbm3data.accessdb.AccessDB): An access db
                initialized with a path to a CBM project.
            project_sim_id (int, optional): [description]. Defaults to None.

        Raises:
            ValueError: raised if a specified project_sim_id does not match a
                simulation id found in the project database.

        Returns:
            int: Returns the Archive index database simulation ID for the
                added project scenario.
        """
        name_tokens = os.path.split(project.path)
        name = name_tokens[len(name_tokens) - 1]
        dir = os.path.dirname(project.path)

        nextInputDBID = self.GetMaxID("tblInputDB", "InputDBID") + 1
        nextStandInitializationID = self.GetMaxID(
            "tblStandInitialization", "StandInitializationID") + 1
        nextCBMRunID = self.GetMaxID("tblCBMRun", "CBMRunID") + 1
        nextSimulationID = self.GetMaxID(
            "tblSimulation", "SimulationID") + 1

        self.InsertRowTo_tblInputDB(dir, name, nextInputDBID)
        if project_sim_id is None:
            project_sim_id = project.GetMaxID("tblSimulation", "SimulationID")

        project_tblSimulationRow = project.Query(
            "SELECT * FROM tblSimulation WHERE "
            "tblSimulation.SimulationID == ?",
            (project_sim_id)).fetchone()
        if not project_tblSimulationRow:
            raise ValueError(
                f"project simulation id {project_sim_id} id not correspond to "
                "an id in project tblSimulation.")

        project_tblRunTableRow = project.Query(
            "SELECT * FROM tblRunTable WHERE "
            "tblRunTable.RunID == ?",
            project_tblSimulationRow.RunID).fetchone()

        project_tblStandInitializationRow = project.Query(
            "SELECT * FROM tblStandInitialization WHERE"
            "tblStandInitialization.StandInitID == ?",
            project_tblSimulationRow.StandInitID).fetchone()

        self.InsertRowTo_tblStandInitialization(
            project_tblStandInitializationRow.Name,
            project_tblStandInitializationRow.Description, " ",
            nextInputDBID, nextStandInitializationID,
            project_tblStandInitializationRow.StandInitID)

        self.InsertRowTo_tblCBMRun(
            project_tblRunTableRow.Name, project_tblRunTableRow.Description,
            " ", nextInputDBID, nextCBMRunID, project_tblRunTableRow.RunID)

        self.InsertRowTo_tblSimulation(
            project_tblSimulationRow.Name,
            project_tblSimulationRow.Description, " ", nextInputDBID,
            nextSimulationID, nextStandInitializationID,
            nextCBMRunID, project_tblSimulationRow.SimulationID)

        return nextSimulationID

    def DeleteProjectsFromAIDB(self, simulation_id=None):
        '''
        Remove a project associated with the given simulation id from the
        AIDB. If no simulation id is given then the aidb will be cleared of
        all projects.
        '''
        if simulation_id is None:
            self.ExecuteQuery("DELETE FROM tblInputDB")
            self.ExecuteQuery("DELETE FROM tblSimulation")
            self.ExecuteQuery("DELETE FROM tblStandInitialization")
            self.ExecuteQuery("DELETE FROM tblCBMRun")
        keys = self.getKeys(simulation_id)
        for item in keys:
            self.ExecuteQuery(
                "DELETE FROM tblInputDB WHERE InputDBID = ?",
                item.InputDBID)

    def getKeys(self, simulation_id=None):

        filterString = None
        if simulation_id is None:
            filterString = ";"
        else:
            filterString = "WHERE tblSimulation.SimulationID = {0};".format(
                int(simulation_id))
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
                    tblInputDB INNER JOIN tblSimulation ON
                        tblInputDB.InputDBID = tblSimulation.InputDBID
                )
                ON tblCBMRun.CBMRunID = tblSimulation.CBMRunID
            )
            ON tblStandInitialization.StandInitializationID =
                tblSimulation.StandInitializationID
            {0}
            """.format(filterString)).fetchall()

        return keys
