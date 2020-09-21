# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada


from cbm3_python.cbm3data.accessdb import AccessDB


class ProjectDB(AccessDB):

    def get_run_length(self, project_simulation_id=None):
        if not project_simulation_id:
            project_simulation_id = self.GetMaxID(
                "tblSimulation", "SimulationID")

        return int(self.Query(
            "SELECT tblRunTableDetails.RunLength "
            "FROM tblSimulation INNER JOIN tblRunTableDetails "
            "ON tblSimulation.RunID = tblRunTableDetails.RunID "
            "WHERE tblSimulation.SimulationID=?;",
            project_simulation_id).fetchone().RunLength)

    def set_run_length(self, run_length, project_simulation_id=None):
        if not project_simulation_id:
            project_simulation_id = self.GetMaxID(
                "tblSimulation", "SimulationID")

        self.ExecuteQuery(
            "UPDATE tblSimulation INNER JOIN tblRunTableDetails "
            "ON tblSimulation.RunID = tblRunTableDetails.RunID "
            "SET tblRunTableDetails.RunLength = ? "
            "WHERE tblSimulation.SimulationID = ?;",
            (run_length, project_simulation_id))
