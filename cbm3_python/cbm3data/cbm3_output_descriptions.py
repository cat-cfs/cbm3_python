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
    aidb_data = SimpleNamespace(
        tblSPUDefault=_query_access_db(
            aidb_path, "SELECT * FROM tblSPUDefault"),
        tblEcoBoundaryDefault=_query_access_db(
            aidb_path, "SELECT * FROM tblEcoBoundaryDefault"),
        tblAdminBoundaryDefault=_query_access_db(
            aidb_path, "SELECT * FROM tblAdminBoundaryDefault"),
        tblDisturbanceTypeDefault=_query_access_db(
            aidb_path, "SELECT * FROM tblDisturbanceTypeDefault"),
        tblUNFCCCLandClass=_query_access_db(
            aidb_path, "SELECT * FROM tblUNFCCCLandClass"),
        tblKP3334Flags=_query_access_db(
            aidb_path, "SELECT * FROM tblKP3334Flags"))

    # note in current build v1.2.7739.338 tblKP3334Flags is missing a row,
    # and the following lines compensate for that
    if len(aidb_data.tblKP3334Flags.index) == 9:
        aidb_data.tblKP3334Flags = pd.DataFrame(
            columns=["KP3334ID", "Name", "Description"],
            data=[[0, "Undetermined", "Undetermined"]]
        ).append(
            aidb_data.tblKP3334Flags)
        aidb_data.tblKP3334Flags = list(range(0, 10))
    return aidb_data


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

        self.project_data = load_project_level_data(project_db_path)
        self.aidb_data = load_archive_index_data(aidb_path)
        self.default_view = self._create_default_data_views()
        self.project_view = self._create_project_data_view()

        self.mapped_csets = self._map_classifier_descriptions(
            loaded_csets, classifier_value_field)

    def _create_default_data_views(self):
        default_spu_view = self.aidb_data.tblSPUDefault[
            ["SPUID", "AdminBoundaryID", "EcoBoundaryID"]].merge(
                self.aidb_data.tblEcoBoundaryDefault[
                    ["EcoBoundaryID", "EcoBoundaryName"]],
                validate="m:1").merge(
                    self.aidb_data.tblAdminBoundaryDefault[
                        ["AdminBoundaryID", "AdminBoundaryName"]],
                    validate="m:1")
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
                ["EcoBoundaryID", "EcoBoundaryName"]],
            validate="1:m").merge(
                self.project_data.tblAdminBoundary[
                    ["AdminBoundaryID", "AdminBoundaryName"]],
                validate="1:m")

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
            self.default_view.default_spu_view, validate="m:1")

        disturbance_type_view = self.project_data.tblDisturbanceType[
            ["DistTypeID", "DistTypeName", "Description", "DefaultDistTypeID"]]
        disturbance_type_view = disturbance_type_view.merge(
            self.default_view.default_disturbance_type_view,
            validate="m:1")
        disturbance_type_view = disturbance_type_view.rename(columns={
            "DistTypeID": "ProjectDistTypeID",
            "DistTypeName": "ProjectDistTypeName",
            "Description": "ProjectDistTypeDescription"
        })
        return SimpleNamespace(
            project_spu_view=project_spu_view,
            disturbance_type_view=disturbance_type_view)

    def _map_classifier_descriptions(self, loaded_csets, description_field):
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
        mapped_csets = loaded_csets.copy()

        for classifier_id, classifier_map in cset_map.items():
            mapped_csets[f"c{classifier_id}"] = \
                mapped_csets[f"c{classifier_id}"].map(classifier_map)
        mapped_csets.columns = \
            [mapped_csets.columns[0]] + \
            list(
                self.project_data.tblClassifiers.sort_values(
                    by="ClassifierID").Name)
        return mapped_csets

    def merge_spatial_unit_description(self, df):
        return self.project_view.project_spu_view.merge(
            df, left_on="ProjectSPUID", right_on="SPUID",
            validate="1:m")

    def merge_disturbance_type_description(self, df):
        return self.project_view.disturbance_type_view.merge(
            df, left_on="ProjectDistTypeID",
            right_on="DistTypeID", how="right", validate="1:m")

    def merge_classifier_set_description(self, df):
        return self.mapped_csets.merge(
            df, left_on="ClassifierSetID",
            right_on="UserDefdClassSetID", validate="1:m")

    def merge_landclass_description(self, df):

        land_class_name = self.aidb_data.tblUNFCCCLandClass.merge(
            df[["LandClassID"]],
            left_on="UNFCCCLandClassID",
            right_on="LandClassID",
            validate="1:m")[["Name"]].rename(
                columns={"Name": "UNFCCCLandClassName"}).set_index(df.index)

        kf3334_name_desc = self.aidb_data.tblKP3334Flags.merge(
            df[["kf2"]],
            left_on=["KP3334ID"],
            right_on=["kf2"],
            validate="1:m")[["Name", "Description"]].rename(
                columns={
                    "Name": "KP3334Name",
                    "Description": "KP3334Description"}).set_index(df.index)

        return land_class_name.merge(
            kf3334_name_desc, left_index=True, right_index=True).merge(
                df, left_index=True, right_index=True, validate="1:1")

    def merge_pool_indicator_descriptions(self, pool_indicators):
        pool_indicators = self.project_view.project_spu_view.merge(
            pool_indicators, left_on="ProjectSPUID", right_on="SPUID")
        pool_indicators = self.mapped_csets.merge(
            pool_indicators, left_on="ClassifierSetID",
            right_on="UserDefdClassSetID", validate="1:m")
        return pool_indicators

    def merge_flux_indicator_descriptions(self, flux_indicators):
        flux_indicators = self.project_view.disturbance_type_view.merge(
            flux_indicators, left_on="ProjectDistTypeID",
            right_on="DistTypeID", how="right", validate="1:m")
        flux_indicators = self.project_view.project_spu_view.merge(
            flux_indicators, left_on="ProjectSPUID", right_on="SPUID",
            validate="1:m")
        flux_indicators = self.mapped_csets.merge(
            flux_indicators, left_on="ClassifierSetID",
            right_on="UserDefdClassSetID", validate="1:m")
        return flux_indicators

    def merge_age_indicator_descriptions(self, age_indicators):
        age_indicators = self.project_view.project_spu_view.merge(
            age_indicators, left_on="ProjectSPUID", right_on="SPUID",
            validate="1:m")
        age_indicators = self.mapped_csets.merge(
            age_indicators, left_on="ClassifierSetID",
            right_on="UserDefdClassSetID",
            validate="1:m")
        return age_indicators

    def merge_dist_indicator_descriptions(self, dist_indicators):
        dist_indicators = self.project_view.disturbance_type_view.merge(
            dist_indicators, left_on="ProjectDistTypeID",
            right_on="DistTypeID", how="right", validate="1:m")
        dist_indicators = self.project_view.project_spu_view.merge(
            dist_indicators, left_on="ProjectSPUID", right_on="SPUID",
            validate="1:m")
        dist_indicators = self.mapped_csets.merge(
            dist_indicators, left_on="ClassifierSetID",
            right_on="UserDefdClassSetID", validate="1:m")
        return dist_indicators
