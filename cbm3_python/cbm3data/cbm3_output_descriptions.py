from types import SimpleNamespace
import pyodbc
import pandas as pd


def _query_access_db(path, query):
    """Return the result as a dataframe the specified query
    on the access database located at the specified path."""
    connection_string = \
        "Driver={Microsoft Access Driver (*.mdb, *.accdb)};" \
        f"User Id='admin';Dbq={path}"

    with pyodbc.connect(connection_string) as connection:
        return pd.read_sql(query, connection)


def load_archive_index_data(aidb_path):
    # load default spu data from archive index
    return SimpleNamespace(
        tblSPUDefault=_query_access_db(
            aidb_path, "SELECT * FROM tblSPUDefault"),
        tblEcoBoundaryDefault=_query_access_db(
            aidb_path, "SELECT * FROM tblEcoBoundaryDefault"),
        tblAdminBoundaryDefault=_query_access_db(
            aidb_path, "SELECT * FROM tblAdminBoundaryDefault"),
        tblDisturbanceTypeDefault=_query_access_db(
            aidb_path, "SELECT * FROM tblDisturbanceTypeDefault"),
        tblUNFCCCLandClass=_query_access_db(
            aidb_path, "SELECT * FROM tblUNFCCCLandClass"))


def load_project_level_data(project_db_path):
    # get project level info
    return SimpleNamespace(
        tblSPU=_query_access_db(
            project_db_path, "SELECT * FROM tblSPU"),
        tblEcoBoundary=_query_access_db(
            project_db_path, "SELECT * FROM tblEcoBoundary"),
        tblAdminBoundary=_query_access_db(
            project_db_path, "SELECT * FROM tblAdminBoundary"),
        tblDisturbanceType=_query_access_db(
            project_db_path, "SELECT * FROM tblDisturbanceType"),
        tblClassifiers=_query_access_db(
            project_db_path, "SELECT * FROM tblClassifiers"),
        tblClassifierValues=_query_access_db(
            project_db_path, "SELECT * FROM tblClassifierValues"),
        tblClassifierSetValues=_query_access_db(
            project_db_path, "SELECT * FROM tblClassifierSetValues"))


class ResultsDescriber():

    def __init__(self, project_db_path, aidb_path, loaded_csets,
                 classifier_value_field="Name"):
        project_data = load_project_level_data(project_db_path)
        aidb_data = load_archive_index_data(aidb_path)
        self.default_view = self._create_default_data_views(aidb_data)
        self.project_view = self._create_project_data_view(
            project_data, self.default_view)

        self.mapped_csets = self._map_classifier_descriptions(
            project_data, loaded_csets, classifier_value_field)

    def _create_default_data_views(self):
        default_spu_view = self.aidb_data.tblSPUDefault[
            ["SPUID", "AdminBoundaryID", "EcoBoundaryID"]].merge(
            self.aidb_data.tblEcoBoundaryDefault[
                ["EcoBoundaryID", "EcoBoundaryName"]]).merge(
            self.aidb_data.tblAdminBoundaryDefault[
                ["AdminBoundaryID", "AdminBoundaryName"]])
        default_spu_view = default_spu_view.rename(columns={
            "SPUID": "DefaultSPUID",
            "AdminBoundaryID": "DefaultAdminBoundaryID",
            "EcoBoundaryID": "DefaultEcoBoundaryID",
            "EcoBoundaryName": "DefaultEcoBoundaryName",
            "AdminBoundaryName": "DefaultAdminBoundaryName"})

        unfccc_land_class_view = self.aidb_data.tblUNFCCCLandClass.rename(
            columns={"Name": "UNFCCCLandClassName"})

        default_disturbance_type_view = \
            self.aidb_data.tblDisturbanceTypeDefault[
                ["DistTypeID", "DistTypeName", "Description"]]

        default_disturbance_type_view = default_disturbance_type_view.rename(
            columns={
                "DistTypeID": "DefaultDistTypeID",
                "DistTypeName": "DefaultDistTypeName",
                "Description": "DefaultDistTypeDescription"
            })
        return SimpleNamespace(
            unfccc_land_class_view=unfccc_land_class_view,
            default_disturbance_type_view=default_disturbance_type_view,
            default_spu_view=default_spu_view)

    def _create_project_data_view(self):
        project_spu_view = self.project_data.tblSPU[
            ["SPUID", "AdminBoundaryID", "EcoBoundaryID", "DefaultSPUID"]
        ].merge(
            self.project_data.tblEcoBoundary[
                ["EcoBoundaryID", "EcoBoundaryName"]]).merge(
            self.project_data.tblAdminBoundary[
                ["AdminBoundaryID", "AdminBoundaryName"]])

        project_spu_view = project_spu_view.rename(
            columns={
                "SPUID": "ProjectSPUID",
                "EcoBoundaryID": "ProjectEcoBoundaryID",
                "EcoBoundaryName": "ProjectEcoBoundaryName",
                "AdminBoundaryID": "ProjectAdminBoundaryID",
                "AdminBoundaryName": "ProjectAdminBoundaryName"})

        # re-order colums
        project_spu_view = project_spu_view[[
            "ProjectSPUID", "ProjectAdminBoundaryID",
            "ProjectEcoBoundaryID", "ProjectEcoBoundaryName",
            "ProjectAdminBoundaryName", "DefaultSPUID"]]

        project_spu_view = project_spu_view.merge(
            self.default_view.default_spu_view)

        disturbance_type_view = self.project_data.tblDisturbanceType[
            ["DistTypeID", "DistTypeName", "Description", "DefaultDistTypeID"]]
        disturbance_type_view = disturbance_type_view.merge(
            self.default_view.default_disturbance_type_view)
        disturbance_type_view = disturbance_type_view.rename(columns={
            "DistTypeID": "ProjectDistTypeID",
            "DistTypeName": "ProjectDistTypeName",
            "Description": "ProjectDistTypeDescription"
        })
        return SimpleNamespace(
            project_spu_view=project_spu_view,
            disturbance_type_view=disturbance_type_view)

    def _map_classifier_descriptions(self, description_field):
        cset_map = {}
        unique_classifier_ids = \
            self.project_data.tblClassifierValues.ClassifierID.unique()
        for classifier in unique_classifier_ids:
            inner_map = {}
            classifier_rows = self.project_data.tblClassifierValues[
                self.project_data.tblClassifierValues.ClassifierID ==
                classifier]
            for _, row in classifier_rows.iterrows():
                inner_map[int(row.ClassifierValueID)] = row[description_field]
            cset_map[int(classifier)] = inner_map
        mapped_csets = self.loaded_csets.copy()

        for classifier_id, classifier_map in cset_map.items():
            mapped_csets[f"c{classifier_id}"] = \
                mapped_csets[f"c{classifier_id}"].map(classifier_map)
        mapped_csets.columns = \
            [mapped_csets.columns[0]] + \
            list(
                self.project_data.tblClassifiers.sort_values(
                    by="ClassifierID").Name)
        return mapped_csets

    def merge_pool_indicator_descriptions(self, pool_indicators):
        pool_indicators = self.project_view.project_spu_view.merge(
            pool_indicators, left_on="ProjectSPUID", right_on="SPUID")
        pool_indicators = self.mapped_csets.merge(
            pool_indicators, left_on="ClassifierSetID",
            right_on="UserDefdClassSetID")
        return pool_indicators

    def merge_flux_indicator_descriptions(self, flux_indicators):
        flux_indicators = self.project_view.disturbance_type_view.merge(
            flux_indicators, left_on="ProjectDistTypeID",
            right_on="DistTypeID", how="right")
        flux_indicators = self.project_view.project_spu_view.merge(
            flux_indicators, left_on="ProjectSPUID", right_on="SPUID")
        flux_indicators = self.mapped_csets.merge(
            flux_indicators, left_on="ClassifierSetID",
            right_on="UserDefdClassSetID")
        return flux_indicators

    def merge_age_indicator_descriptions(self, age_indicators):
        age_indicators = self.project_view.project_spu_view.merge(
            age_indicators, left_on="ProjectSPUID", right_on="SPUID")
        age_indicators = self.mapped_csets.merge(
            age_indicators, left_on="ClassifierSetID",
            right_on="UserDefdClassSetID")
        return age_indicators

    def merge_dist_indicator_descriptions(self, dist_indicators):
        dist_indicators = self.project_view.disturbance_type_view.merge(
            dist_indicators, left_on="ProjectDistTypeID",
            right_on="DistTypeID", how="right")
        dist_indicators = self.project_view.project_spu_view.merge(
            dist_indicators, left_on="ProjectSPUID", right_on="SPUID")
        dist_indicators = self.mapped_csets.merge(
            dist_indicators, left_on="ClassifierSetID",
            right_on="UserDefdClassSetID")
        return dist_indicators