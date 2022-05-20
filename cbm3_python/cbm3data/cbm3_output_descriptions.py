import os
from types import SimpleNamespace
from cbm3_python.cbm3data import accessdb
import pandas as pd
from warnings import warn


def _load_substituted(aidb_path, table_name):
    """workaround for older versions of archive index that do not have
    certain tables.  If the table is present, return that table,
    otherwise load a packaged copy of the table (packaged csv file)

    A warning.warn will be called if the specified table is loaded from the
    substitute csv file.

    Args:
        aidb_path (str): path to the archive index
        table_name (str): name of the table to fetch

    Returns:
        pandas.DataFrame: the resulting table derived from the aidb or csv
            file.
    """
    with accessdb.AccessDB(aidb_path, False) as aidb:
        if aidb.tableExists(table_name):
            return pd.read_sql(f"SELECT * FROM {table_name}", aidb.connection)
        else:
            warn(
                f"{table_name} not found in archive index database "
                f"'{aidb_path}' loading packaged substitute"
            )
            path = os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                f"{table_name}.csv",
            )
            return pd.read_csv(path)


def load_archive_index_data(aidb_path):
    """Loads descriptive/metadata tables from the specified CBM3 MS access
    archive index database path.

    Args:
        aidb_path (str): Path to a CBM-CFS3 archive index database.

    Returns:
        namespace: A namespace of descriptive pandas.DataFrame
    """

    aidb_data = SimpleNamespace(
        tblEcoBoundaryDefault=accessdb.as_data_frame(
            "SELECT EcoBoundaryID, EcoBoundaryName "
            "FROM tblEcoBoundaryDefault",
            aidb_path,
        ),
        tblAdminBoundaryDefault=accessdb.as_data_frame(
            "SELECT AdminBoundaryID, AdminBoundaryName "
            "FROM tblAdminBoundaryDefault",
            aidb_path,
        ),
        tblSPUDefault=accessdb.as_data_frame(
            "SELECT SPUID, AdminBoundaryID, EcoBoundaryID "
            "FROM tblSPUDefault",
            aidb_path,
        ),
        tblDisturbanceTypeDefault=accessdb.as_data_frame(
            "SELECT DistTypeID, DistTypeName, Description "
            "FROM tblDisturbanceTypeDefault",
            aidb_path,
        ),
        tblUNFCCCLandClass=_load_substituted(aidb_path, "tblUNFCCCLandClass"),
        tblKP3334Flags=_load_substituted(aidb_path, "tblKP3334Flags"),
    )

    # note in current build v1.2.7739.338 tblKP3334Flags is missing a row,
    # and the following lines compensate for that
    if len(aidb_data.tblKP3334Flags.index) == 9:
        aidb_data.tblKP3334Flags = pd.DataFrame(
            columns=["KP3334ID", "Name", "Description"],
            data=[[0, "Undetermined", "Undetermined"]],
        ).append(aidb_data.tblKP3334Flags)
        aidb_data.tblKP3334Flags.KP3334ID = list(range(0, 10))
    return aidb_data


def load_project_level_data(project_db_path):
    """Loads descriptive/metadata tables from the specified CBM3 MS access
    project database path.

    Args:
        project_db_path (str): Path to a CBM-CFS3 project database

    Returns:
        namespace: A namespace of descriptive pandas.DataFrames
    """
    tblDisturbanceType = accessdb.as_data_frame(
        "SELECT DistTypeID, DistTypeName, Description, DefaultDistTypeID "
        "FROM tblDisturbanceType",
        project_db_path,
    )
    if not (tblDisturbanceType.DistTypeID == 0).any():
        # add disturbance type 0
        tblDisturbanceType = (
            pd.DataFrame(
                columns=[
                    "DistTypeID",
                    "DistTypeName",
                    "Description",
                    "DefaultDistTypeID",
                ],
                data=[[0, "Annual Processes", "Annual Processes", 0]],
            )
            .append(tblDisturbanceType)
            .reset_index(drop=True)
        )

    return SimpleNamespace(
        tblEcoBoundary=accessdb.as_data_frame(
            "SELECT * FROM tblEcoBoundary", project_db_path
        ),
        tblAdminBoundary=accessdb.as_data_frame(
            "SELECT * FROM tblAdminBoundary", project_db_path
        ),
        tblSPU=accessdb.as_data_frame(
            "SELECT SPUID, AdminBoundaryID, EcoBoundaryID, DefaultSPUID "
            "FROM tblSPU",
            project_db_path,
        ),
        tblSPUGroup=accessdb.as_data_frame(
            "SELECT * from tblSPUGroup", project_db_path
        ),
        tblSPUGroupLookup=accessdb.as_data_frame(
            "SELECT * from tblSPUGroupLookup", project_db_path
        ),
        tblDisturbanceType=tblDisturbanceType,
        tblClassifiers=accessdb.as_data_frame(
            "SELECT ClassifierID, Name FROM tblClassifiers", project_db_path
        ),
        tblClassifierValues=accessdb.as_data_frame(
            "SELECT * FROM tblClassifierValues", project_db_path
        ),
        tblClassifierSetValues=accessdb.as_data_frame(
            "SELECT * FROM tblClassifierSetValues", project_db_path
        ),
        tblClassifierAggregates=accessdb.as_data_frame(
            "SELECT * FROM tblClassifierAggregate", project_db_path
        ),
    )


def create_project_level_output_tables(project_descriptions):
    """creates the equivalent column/table naming as the CBM3
    run results db format for classifier tables

    Args:
        project_descriptions (object): object containing metadata tables
            processed/collect from the project db.
    """
    result = SimpleNamespace(
        tblEcoBoundary=project_descriptions.tblEcoBoundary,
        tblAdminBoundary=project_descriptions.tblAdminBoundary,
        tblSPU=project_descriptions.tblSPU,
        tblSPUGroup=project_descriptions.tblSPUGroup,
        tblSPUGroupLookup=project_descriptions.tblSPUGroupLookup,
        tblDisturbanceType=project_descriptions.tblDisturbanceType,
    )
    result.tblUserDefdClasses = project_descriptions.tblClassifiers.rename(
        columns={"ClassifierID": "UserDefdClassID", "Name": "ClassDesc"}
    )
    result.tblUserDefdSubclasses = (
        project_descriptions.tblClassifierValues.rename(
            columns={
                "ClassifierID": "UserDefdClassID",
                "ClassifierValueID": "UserDefdSubclassID",
                "Name": "UserDefdSubClassName",
                "Description": "SubclassDesc",
            }
        )
    )
    result.tblUserDefdClassSets = (
        project_descriptions.tblClassifierSets.rename(
            columns={"ClassifierSetID": "UserDefdClassSetID"}
        )
    )
    result.tblUserDefdClassSetValues = (
        project_descriptions.tblClassifierSetValues.rename(
            columns={
                "ClassifierSetID": "UserDefdClassSetID",
                "ClassifierID": "UserDefdClassID",
                "ClassifierValueID": "UserDefdSubClassID",
            }
        )
    )
    return result


def load_age_classes():
    """Loads the tblAgeClasses metadata table

    Returns:
        pandas.DataFrame: tblAgeClasses
    """
    path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "tblAgeClasses.csv"
    )
    return pd.read_csv(path)


class ResultsDescriber:
    def __init__(
        self,
        project_db_path,
        aidb_path,
        loaded_csets,
        classifier_value_field="Name",
    ):

        self.project_data = load_project_level_data(project_db_path)
        self.aidb_data = load_archive_index_data(aidb_path)
        self.default_view = self._create_default_data_views()
        self.project_view = self._create_project_data_view()
        self.age_classes = load_age_classes()
        self.mapped_csets = self._map_classifier_descriptions(
            loaded_csets, classifier_value_field
        )

    def _create_default_data_views(self):
        default_spu_view = (
            self.aidb_data.tblSPUDefault[
                ["SPUID", "AdminBoundaryID", "EcoBoundaryID"]
            ]
            .merge(
                self.aidb_data.tblEcoBoundaryDefault[
                    ["EcoBoundaryID", "EcoBoundaryName"]
                ],
                validate="m:1",
                sort=False,
                how="left",
            )
            .merge(
                self.aidb_data.tblAdminBoundaryDefault[
                    ["AdminBoundaryID", "AdminBoundaryName"]
                ],
                validate="m:1",
                sort=False,
                how="left",
            )
        )
        default_spu_view = default_spu_view.rename(
            columns={
                "SPUID": "DefaultSPUID",
                "AdminBoundaryID": "DefaultAdminBoundaryID",
                "EcoBoundaryID": "DefaultEcoBoundaryID",
                "EcoBoundaryName": "DefaultEcoBoundaryName",
                "AdminBoundaryName": "DefaultAdminBoundaryName",
            }
        )

        unfccc_land_class_view = self.aidb_data.tblUNFCCCLandClass.rename(
            columns={"Name": "UNFCCCLandClassName"}
        )

        default_disturbance_type_view = (
            self.aidb_data.tblDisturbanceTypeDefault[
                ["DistTypeID", "DistTypeName", "Description"]
            ]
        )

        default_disturbance_type_view = default_disturbance_type_view.rename(
            columns={
                "DistTypeID": "DefaultDistTypeID",
                "DistTypeName": "DefaultDistTypeName",
                "Description": "DefaultDistTypeDescription",
            }
        )
        return SimpleNamespace(
            unfccc_land_class_view=unfccc_land_class_view,
            default_disturbance_type_view=default_disturbance_type_view,
            default_spu_view=default_spu_view,
        )

    def _create_project_data_view(self):
        project_spu_view = (
            self.project_data.tblSPU[
                ["SPUID", "AdminBoundaryID", "EcoBoundaryID", "DefaultSPUID"]
            ]
            .merge(
                self.project_data.tblEcoBoundary[
                    ["EcoBoundaryID", "EcoBoundaryName"]
                ],
                validate="m:1",
                sort=False,
                how="left",
            )
            .merge(
                self.project_data.tblAdminBoundary[
                    ["AdminBoundaryID", "AdminBoundaryName"]
                ],
                validate="m:1",
                sort=False,
                how="left",
            )
        )

        project_spu_view = project_spu_view.rename(
            columns={
                "SPUID": "ProjectSPUID",
                "EcoBoundaryID": "ProjectEcoBoundaryID",
                "EcoBoundaryName": "ProjectEcoBoundaryName",
                "AdminBoundaryID": "ProjectAdminBoundaryID",
                "AdminBoundaryName": "ProjectAdminBoundaryName",
            }
        )

        # re-order colums
        project_spu_view = project_spu_view[
            [
                "ProjectSPUID",
                "ProjectAdminBoundaryID",
                "ProjectEcoBoundaryID",
                "ProjectEcoBoundaryName",
                "ProjectAdminBoundaryName",
                "DefaultSPUID",
            ]
        ]

        project_spu_view = project_spu_view.merge(
            self.default_view.default_spu_view,
            validate="m:1",
            sort=False,
            how="left",
        )

        disturbance_type_view = self.project_data.tblDisturbanceType[
            ["DistTypeID", "DistTypeName", "Description", "DefaultDistTypeID"]
        ]
        disturbance_type_view = disturbance_type_view.merge(
            self.default_view.default_disturbance_type_view,
            validate="m:1",
            sort=False,
            how="left",
        )
        disturbance_type_view = disturbance_type_view.rename(
            columns={
                "DistTypeID": "ProjectDistTypeID",
                "DistTypeName": "ProjectDistTypeName",
                "Description": "ProjectDistTypeDescription",
            }
        )
        return SimpleNamespace(
            project_spu_view=project_spu_view,
            disturbance_type_view=disturbance_type_view,
        )

    def _map_classifier_descriptions(self, loaded_csets, description_field):
        cset_map = {}
        unique_classifier_ids = (
            self.project_data.tblClassifierValues.ClassifierID.unique()
        )
        for classifier in unique_classifier_ids:
            inner_map = {}
            classifier_rows = self.project_data.tblClassifierValues[
                self.project_data.tblClassifierValues.ClassifierID
                == classifier
            ]
            for _, row in classifier_rows.iterrows():
                inner_map[int(row.ClassifierValueID)] = row[description_field]
            cset_map[int(classifier)] = inner_map
        mapped_csets = loaded_csets.copy()

        for classifier_id, classifier_map in cset_map.items():
            mapped_csets[f"c{classifier_id}"] = mapped_csets[
                f"c{classifier_id}"
            ].map(classifier_map)
        mapped_csets.columns = [mapped_csets.columns[0]] + list(
            self.project_data.tblClassifiers.sort_values(
                by="ClassifierID"
            ).Name
        )
        return mapped_csets

    def merge_spatial_unit_description(self, df):
        """Merges spatial unit metadata columns to a dataframe containing
        a project-level SPUID column.

        Args:
            df (pandas.DataFrame): a table containing column "SPUID"

        Returns:
            pandas.DataFrame: the merged table (copied)
        """
        return self.project_view.project_spu_view.merge(
            df,
            left_on="ProjectSPUID",
            right_on="SPUID",
            validate="1:m",
            sort=False,
            how="right",
        )

    def merge_disturbance_type_description(self, df):
        """Merges disturbance type metadata columns to a dataframe containing
        a project-level DistTypeID column.

        Args:
            df (pandas.DataFrame): a table containing column "DistTypeID"

        Returns:
            pandas.DataFrame: the merged table (copied)
        """
        return self.project_view.disturbance_type_view.merge(
            df,
            left_on="ProjectDistTypeID",
            right_on="DistTypeID",
            validate="1:m",
            sort=False,
            how="right",
        )

    def merge_classifier_set_description(self, df):
        """Merges classifier value columns to a dataframe containing
        a project-level UserDefdClassSetID column.

        Args:
            df (pandas.DataFrame): a table containing column
                "UserDefdClassSetID"

        Returns:
            pandas.DataFrame: the merged table (copied)
        """
        return self.mapped_csets.merge(
            df,
            left_on="ClassifierSetID",
            right_on="UserDefdClassSetID",
            validate="1:m",
            sort=False,
            how="right",
        )

    def merge_landclass_description(self, df):
        """Merges UNFCCC land class metadata columns to a dataframe containing
        project-level LandClassID and kf2 columns.

        Args:
            df (pandas.DataFrame): a table containing columns "LandClassID"
                and "kf2".

        Returns:
            pandas.DataFrame: the merged table (copied)
        """

        land_class_name = (
            self.aidb_data.tblUNFCCCLandClass.merge(
                df[["LandClassID"]],
                left_on="UNFCCCLandClassID",
                right_on="LandClassID",
                validate="1:m",
                sort=False,
                how="right",
            )[["Name"]]
            .rename(columns={"Name": "UNFCCCLandClassName"})
            .set_index(df.index)
        )

        kf3334_name_desc = (
            self.aidb_data.tblKP3334Flags.merge(
                df[["kf2"]],
                left_on=["KP3334ID"],
                right_on=["kf2"],
                validate="1:m",
                sort=False,
                how="right",
            )[["Name", "Description"]]
            .rename(
                columns={
                    "Name": "KP3334Name",
                    "Description": "KP3334Description",
                }
            )
            .set_index(df.index)
        )

        return land_class_name.merge(
            kf3334_name_desc, left_index=True, right_index=True, sort=False
        ).merge(
            df,
            left_index=True,
            right_index=True,
            sort=False,
            validate="1:1",
            how="right",
        )

    def merge_age_class_descriptions(self, df):
        """Merges age class metadata columns to a dataframe containing
        a project-level AgeClassID column

        Args:
            df (pandas.DataFrame): a table containing column "AgeClassID"

        Returns:
            pandas.DataFrame: the merged table (copied)
        """
        return self.age_classes.merge(
            df,
            left_on="AgeClassID",
            right_on="AgeClassID",
            sort=False,
            validate="1:m",
            how="right",
        )
