import sqlalchemy
from sqlalchemy import ForeignKey
from sqlalchemy import Column


def _get_constraints(
    primary_key=None, index=None, unique=None, foreign_key=None
):
    args = []
    kwargs = {}
    if primary_key:
        kwargs.update({"primary_key": primary_key})
    if index:
        kwargs.update({"index": index})
    if unique:
        kwargs.update({"unique": unique})
    if foreign_key:
        args.append(ForeignKey(foreign_key))
    return dict(args=args, kwargs=kwargs)


def get_constraints():
    """Get a nested dictionary of table names to column
    dictionaries containing constraint and index definitions for sqlalchemy

    Returns:
        dict: the column constraints and indexes
    """
    return {
        "tblAccountingRuleDiagnostics": {
            "DistTypeID": _get_constraints(
                foreign_key="tblDisturbanceType.DistTypeID", index=True
            )
        },
        "tblAdminBoundary": {
            "AdminBoundaryID": _get_constraints(primary_key=True),
            # some projects have duplicate names!
            # "AdminBoundaryName": _get_constraints(unique=True),
            "DefaultAdminBoundaryID": _get_constraints(
                foreign_key="tblAdminBoundaryDefault.AdminBoundaryID"
            ),
        },
        "tblAdminBoundaryDefault": {
            "AdminBoundaryID": _get_constraints(primary_key=True),
            "AdminBoundaryName": _get_constraints(unique=True),
        },
        "tblAgeClasses": {
            "AgeClassID": _get_constraints(primary_key=True),
            "AgeRange": _get_constraints(unique=True),
        },
        "tblAgeIndicators": {
            "AgeIndID": _get_constraints(primary_key=True),
            "AgeClassID": _get_constraints(
                foreign_key="tblAgeClasses.AgeClassID"
            ),
            "TimeStep": _get_constraints(index=True),
            "SPUID": _get_constraints(index=True, foreign_key="tblSPU.SPUID"),
            # "AgeClassID": None,  # need to add additional table!
            "UserDefdClassSetID": _get_constraints(
                index=True,
                foreign_key="tblUserDefdClassSets.UserDefdClassSetID",
            ),
            "LandClassID": _get_constraints(
                index=True, foreign_key="tblUNFCCCLandClass.UNFCCCLandClassID"
            ),
            "kf2": _get_constraints(
                index=True, foreign_key="tblKP3334Flags.KP3334ID"
            ),
            "kf3": _get_constraints(index=True),
            "kf4": _get_constraints(index=True),
            "kf5": _get_constraints(index=True),
            "kf6": _get_constraints(index=True),
        },
        "tblUserDefdClasses": {
            "UserDefdClassID": _get_constraints(primary_key=True),
            "ClassDesc": _get_constraints(unique=True),
        },
        "tblUserDefdSubclasses": {
            "UserDefdClassID": _get_constraints(
                foreign_key="tblUserDefdClasses.UserDefdClassID",
                index=True,
                primary_key=True,
            ),
            "UserDefdSubclassID": _get_constraints(
                index=True, primary_key=True
            ),
        },
        "tblUserDefdClassSets": {
            "UserDefdClassSetID": _get_constraints(
                primary_key=True, index=True
            )
        },
        "tblUserDefdClassSetValues": {
            "UserDefdClassSetID": _get_constraints(
                foreign_key="tblUserDefdClassSets.UserDefdClassSetID",
                index=True,
                primary_key=True,
            ),
            "UserDefdClassID": _get_constraints(primary_key=True, index=True),
            "UserDefdSubClassID": _get_constraints(
                primary_key=True, index=True
            ),
        },
        "tblDistIndicators": {
            "DistIndID": _get_constraints(primary_key=True),
            "TimeStep": _get_constraints(index=True),
            "DistTypeID": _get_constraints(
                foreign_key="tblDisturbanceType.DistTypeID", index=True
            ),
            "SPUID": _get_constraints(index=True, foreign_key="tblSPU.SPUID"),
            "UserDefdClassSetID": _get_constraints(
                index=True,
                foreign_key="tblUserDefdClassSets.UserDefdClassSetID",
            ),
            "LandClassID": _get_constraints(
                index=True, foreign_key="tblUNFCCCLandClass.UNFCCCLandClassID"
            ),
            "kf2": _get_constraints(
                index=True, foreign_key="tblKP3334Flags.KP3334ID"
            ),
            "kf3": _get_constraints(index=True),
            "kf4": _get_constraints(index=True),
            "kf5": _get_constraints(index=True),
            "kf6": _get_constraints(index=True),
        },
        "tblDistNotRealized": {
            "DistTypeID": _get_constraints(
                foreign_key="tblDisturbanceType.DistTypeID", index=True
            ),
        },
        "tblDisturbanceReconciliation": {
            "DisturbanceEventID": _get_constraints(primary_key=True),
            "DisturbanceGroupScenarioID": _get_constraints(index=True),
            "DistTypeID": _get_constraints(
                foreign_key="tblDisturbanceType.DistTypeID", index=True
            ),
        },
        "tblDisturbanceType": {
            "DistTypeID": _get_constraints(primary_key=True),
            # some projects have duplicate names!
            # "DistTypeName": _get_constraints(unique=True),
            "DefaultDistTypeID": _get_constraints(
                foreign_key="tblDisturbanceTypeDefault.DistTypeID", index=True
            ),
        },
        "tblDisturbanceTypeDefault": {
            "DistTypeID": _get_constraints(primary_key=True),
            "DistTypeName": _get_constraints(unique=True),
        },
        "tblEcoBoundary": {
            "EcoBoundaryID": _get_constraints(primary_key=True),
            # "EcoBoundaryName": _get_constraints(unique=True),
            "DefaultEcoBoundaryID": _get_constraints(
                foreign_key="tblEcoBoundaryDefault.EcoBoundaryID", index=True
            ),
        },
        "tblEcoBoundaryDefault": {
            "EcoBoundaryID": _get_constraints(primary_key=True),
            "EcoBoundaryName": _get_constraints(unique=True),
        },
        "tblFluxIndicators": {
            "FluxIndicatorID": _get_constraints(primary_key=True),
            "DistTypeID": _get_constraints(
                foreign_key="tblDisturbanceType.DistTypeID", index=True
            ),
            "SPUID": _get_constraints(index=True, foreign_key="tblSPU.SPUID"),
            "TimeStep": _get_constraints(index=True),
            "UserDefdClassSetID": _get_constraints(
                index=True,
                foreign_key="tblUserDefdClassSets.UserDefdClassSetID",
            ),
            "LandClassID": _get_constraints(
                index=True, foreign_key="tblUNFCCCLandClass.UNFCCCLandClassID"
            ),
            "kf2": _get_constraints(
                index=True, foreign_key="tblKP3334Flags.KP3334ID"
            ),
            "kf3": _get_constraints(index=True),
            "kf4": _get_constraints(index=True),
            "kf5": _get_constraints(index=True),
            "kf6": _get_constraints(index=True),
        },
        "tblKP3334Flags": {
            "KP3334ID": _get_constraints(primary_key=True),
            "Name": _get_constraints(unique=True),
        },
        "tblNIRSpecialOutput": {
            "usLessPkField": _get_constraints(primary_key=True),
            "SPUID": _get_constraints(index=True, foreign_key="tblSPU.SPUID"),
            "TimeStep": _get_constraints(index=True),
            "DistTypeID": _get_constraints(
                foreign_key="tblDisturbanceType.DistTypeID", index=True
            ),
            "LandClass_From": _get_constraints(
                index=True, foreign_key="tblUNFCCCLandClass.UNFCCCLandClassID"
            ),
            "LandClass_To": _get_constraints(
                index=True, foreign_key="tblUNFCCCLandClass.UNFCCCLandClassID"
            ),
        },
        "tblPoolIndicators": {
            "PoolIndID": _get_constraints(primary_key=True),
            "SPUID": _get_constraints(index=True, foreign_key="tblSPU.SPUID"),
            "TimeStep": _get_constraints(index=True),
            "UserDefdClassSetID": _get_constraints(
                index=True,
                foreign_key="tblUserDefdClassSets.UserDefdClassSetID",
            ),
            "LandClassID": _get_constraints(
                index=True, foreign_key="tblUNFCCCLandClass.UNFCCCLandClassID"
            ),
            "kf2": _get_constraints(
                index=True, foreign_key="tblKP3334Flags.KP3334ID"
            ),
            "kf3": _get_constraints(index=True),
            "kf4": _get_constraints(index=True),
            "kf5": _get_constraints(index=True),
            "kf6": _get_constraints(index=True),
        },
        "tblPreDisturbanceAge": {
            "PreDistAgeID": _get_constraints(primary_key=True),
            "SPUID": _get_constraints(index=True, foreign_key="tblSPU.SPUID"),
            "TimeStep": _get_constraints(index=True),
            "DistTypeID": _get_constraints(
                foreign_key="tblDisturbanceType.DistTypeID", index=True
            ),
            "UserDefdClassSetID": _get_constraints(
                index=True,
                foreign_key="tblUserDefdClassSets.UserDefdClassSetID",
            ),
            "LandClassID": _get_constraints(
                index=True, foreign_key="tblUNFCCCLandClass.UNFCCCLandClassID"
            ),
            "kf2": _get_constraints(
                index=True, foreign_key="tblKP3334Flags.KP3334ID"
            ),
            "kf3": _get_constraints(index=True),
            "kf4": _get_constraints(index=True),
            "kf5": _get_constraints(index=True),
            "kf6": _get_constraints(index=True),
        },
        "tblSPU": {
            "SPUID": _get_constraints(primary_key=True),
            "AdminBoundaryID": _get_constraints(
                foreign_key="tblAdminBoundary.AdminBoundaryID", index=True
            ),
            "EcoBoundaryID": _get_constraints(
                foreign_key="tblEcoBoundary.EcoBoundaryID", index=True
            ),
            "DefaultSPUID": _get_constraints(
                foreign_key="tblSPUDefault.SPUID", index=True
            ),
        },
        "tblSPUDefault": {
            "SPUID": _get_constraints(primary_key=True),
            "AdminBoundaryID": _get_constraints(
                foreign_key="tblAdminBoundaryDefault.AdminBoundaryID",
                index=True,
            ),
            "EcoBoundaryID": _get_constraints(
                foreign_key="tblEcoBoundaryDefault.EcoBoundaryID", index=True
            ),
        },
        "tblSVL": {
            "SVLID": _get_constraints(primary_key=True),
            "SPUID": _get_constraints(index=True, foreign_key="tblSPU.SPUID"),
            "TimeStep": _get_constraints(index=True),
            "DistTypeID": _get_constraints(
                foreign_key="tblDisturbanceType.DistTypeID", index=True
            ),
            "UserDefdClassSetID": _get_constraints(
                index=True,
                foreign_key="tblUserDefdClassSets.UserDefdClassSetID",
            ),
            "LandClassID": _get_constraints(
                index=True, foreign_key="tblUNFCCCLandClass.UNFCCCLandClassID"
            ),
            "kf2": _get_constraints(
                index=True, foreign_key="tblKP3334Flags.KP3334ID"
            ),
            "kf3": _get_constraints(index=True),
            "kf4": _get_constraints(index=True),
            "kf5": _get_constraints(index=True),
            "kf6": _get_constraints(index=True),
        },
        "tblUNFCCCLandClass": {
            "UNFCCCLandClassID": _get_constraints(primary_key=True),
            "Name": _get_constraints(index=True, unique=True),
        },
    }


def _map_pandas_dtype(dtype):
    _dtype_str = str(dtype).lower()
    if _dtype_str == "int64":
        return sqlalchemy.Integer
    elif _dtype_str == "float64":
        return sqlalchemy.Float
    elif _dtype_str == "object":
        return sqlalchemy.String
    elif _dtype_str == "string":
        return sqlalchemy.String
    elif _dtype_str == "bool":
        return sqlalchemy.Boolean
    else:
        raise ValueError(f"unmapped type {dtype}")


def _unpack_constraint_schema(constraint_defs, name, column):
    args = None
    kwargs = None
    if name in constraint_defs:
        defs = constraint_defs[name]
        if column in defs:
            column_defs = defs[column]
            if "args" in column_defs:
                args = column_defs["args"]
            if "kwargs" in column_defs:
                kwargs = column_defs["kwargs"]
    return args, kwargs


def create_column_definitions(name, df, constraint_defs):
    """Create the column definitions to pass to an sqlalchemy engine
    for table construction.

    Args:
        name (str): the table name
        df (pandas.DataFrame): a table from which the column names and
            types will be derived
        constraint_defs (dict): dictionary of constraint and index
            definitions. See :py:func:`get_constraints`

    Returns:
        list: list of sqlalchemy.Column
    """
    result = []
    for i_column, column in enumerate(df.columns):
        column_args = [column, _map_pandas_dtype(df.dtypes.iloc[i_column])]
        column_kwargs = {}

        (
            column_constraint_args,
            column_constraint_kwargs,
        ) = _unpack_constraint_schema(constraint_defs, name, column)
        if column_constraint_args:
            column_args.extend(column_constraint_args)
        if column_constraint_kwargs:
            column_kwargs.update(column_constraint_kwargs)

        result.append(Column(*column_args, **column_kwargs))
    return result
