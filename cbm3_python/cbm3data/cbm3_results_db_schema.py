from numpy.lib.arraysetops import unique
import sqlalchemy
from sqlalchemy import ForeignKey
from sqlalchemy import Column


def _get_constraints(primary_key=None, index=None,
                     unique=None, foreign_key=None):
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


CBM_RESULTS_CONSTRAINT_DEFS = {
    "tblAccountingRuleDiagnostics": {
        "DistTypeID": _get_constraints(
            foreign_key="tblDisturbanceType.DistTypeID", index=True)
    },
    "tblAdminBoundary": {
        "AdminBoundaryID": _get_constraints(primary_key=True),
        "AdminBoundaryName": _get_constraints(unique=True),
        "DefaultAdminBoundaryID": _get_constraints(
            foreign_key="tblAdminBoundaryDefault.AdminBoundaryID")
    },
    "tblAdminBoundaryDefault": {
        "AdminBoundaryID": _get_constraints(primary_key=True),
        "AdminBoundaryName": _get_constraints(unique=True),
    },
    "tblAgeIndicators": {
        "AgeIndID": _get_constraints(primary_key=True),
        "TimeStep":  _get_constraints(index=True),
        "SPUID": _get_constraints(index=True, foreign_key="tblSPU.SPUID"),
        # "AgeClassID": None,  # need to add additional table!
        "UserDefdClassSetID": _get_constraints(
            index=True, foreign_key="tblClassifierSetValues.ClassifierSetID"),
        "LandClassID": _get_constraints(
            index=True, foreign_key="tblUNFCCCLandClass.UNFCCCLandClassID"),
        "kf2": _get_constraints(
            index=True, foreign_key="tblKP3334Flags.KP3334ID"),
        "kf3": _get_constraints(index=True),
        "kf4": _get_constraints(index=True),
        "kf5": _get_constraints(index=True),
        "kf6": _get_constraints(index=True),
    },
    "tblClassifierSetValues": {
        "ClassifierSetID": _get_constraints(index=True),
        "ClassifierID": _get_constraints(index=True),
        "ClassifierValueID": _get_constraints(index=True),
    },
    "tblClassifierValues": {
        "ClassifierID": _get_constraints(index=True),
        "ClassifierValueID": _get_constraints(index=True)
    },
    "tblClassifiers": {
        "ClassifierID": _get_constraints(primary_key=True),
        "Name": _get_constraints(unique=True),
    },
    "tblDistIndicators": {
        "DistIndID": _get_constraints(primary_key=True),
        "TimeStep": _get_constraints(index=True),
        "DistTypeID": _get_constraints(
            foreign_key="tblDisturbanceType.DistTypeID", index=True),
        "SPUID": _get_constraints(index=True, foreign_key="tblSPU.SPUID"),
        "UserDefdClassSetID": _get_constraints(
            index=True, foreign_key="tblClassifierSetValues.ClassifierSetID"),
        "LandClassID": _get_constraints(
            index=True, foreign_key="tblUNFCCCLandClass.UNFCCCLandClassID"),
        "kf2": _get_constraints(
            index=True, foreign_key="tblKP3334Flags.KP3334ID"),
        "kf3": _get_constraints(index=True),
        "kf4": _get_constraints(index=True),
        "kf5": _get_constraints(index=True),
        "kf6": _get_constraints(index=True),
    },
    "tblDistNotRealized": {
        "DistTypeID": _get_constraints(
            foreign_key="tblDisturbanceType.DistTypeID", index=True),
    },
    "tblDisturbanceReconciliation": {
        "DisturbanceEventID": _get_constraints(primary_key=True),
        "DisturbanceGroupScenarioID": _get_constraints(index=True),
        "DistTypeID": _get_constraints(
            foreign_key="tblDisturbanceType.DistTypeID", index=True),
    },
    "tblDisturbanceType": {
        "DistTypeID": _get_constraints(primary_key=True),
        "DistTypeName": _get_constraints(unique=True),
        "DefaultDistTypeID": _get_constraints(
            foreign_key="tblDisturbanceTypeDefault.DistTypeID", index=True),
    },
    "tblDisturbanceTypeDefault": {
        "DistTypeID": _get_constraints(primary_key=True),
        "DistTypeName": _get_constraints(unique=True),
    },
    "tblEcoBoundary": {
        "EcoBoundaryID": _get_constraints(primary_key=True),
        "EcoBoundaryName": _get_constraints(unique=True),
        "DefaultEcoBoundaryID": _get_constraints(
            foreign_key="tblEcoBoundaryDefault.EcoBoundaryID", index=True)
    },
    "tblEcoBoundaryDefault": {
        "EcoBoundaryID": _get_constraints(primary_key=True),
        "EcoBoundaryName": _get_constraints(unique=True),
    },
    "tblFluxIndicators": {
        "FluxIndicatorID": _get_constraints(primary_key=True),
        "DistTypeID": _get_constraints(
            foreign_key="tblDisturbanceType.DistTypeID", index=True),
        "SPUID": _get_constraints(index=True, foreign_key="tblSPU.SPUID"),
        "TimeStep": _get_constraints(index=True),
        "UserDefdClassSetID": _get_constraints(
            index=True, foreign_key="tblClassifierSetValues.ClassifierSetID"),
        "LandClassID": _get_constraints(
            index=True, foreign_key="tblUNFCCCLandClass.UNFCCCLandClassID"),
        "kf2": _get_constraints(
            index=True, foreign_key="tblKP3334Flags.KP3334ID"),
        "kf3": _get_constraints(index=True),
        "kf4": _get_constraints(index=True),
        "kf5": _get_constraints(index=True),
        "kf6": _get_constraints(index=True)
    },
    "tblKP3334Flags": {
        "KP3334ID": _get_constraints(primary_key=True),
        "Name": _get_constraints(unique=True),
    },
    "tblNIRSpecialOutput": {
        "NIRSpecialOutputID": _get_constraints(primary_key=True),
        "SPUID": _get_constraints(index=True, foreign_key="tblSPU.SPUID"),
        "TimeStep": None,  # index
        "DistTypeID": {
            "args": [ForeignKey("tblDisturbanceType.DistTypeID")],
            "kwargs": {"index": True}
        },
        "LandClass_From": None,  # fk, index
        "LandClass_To": None,  # fk, index
    },
    "tblPoolIndicators": {
        "PoolIndID": _get_constraints(primary_key=True),
        "SPUID": _get_constraints(index=True, foreign_key="tblSPU.SPUID"),
        "TimeStep": _get_constraints(index=True),
        "UserDefdClassSetID": _get_constraints(
            index=True, foreign_key="tblClassifierSetValues.ClassifierSetID"),
        "LandClassID": _get_constraints(
            index=True, foreign_key="tblUNFCCCLandClass.UNFCCCLandClassID"),
        "kf2": _get_constraints(
            index=True, foreign_key="tblKP3334Flags.KP3334ID"),
        "kf3": _get_constraints(index=True),
        "kf4": _get_constraints(index=True),
        "kf5": _get_constraints(index=True),
        "kf6": _get_constraints(index=True)
    },
    "tblPreDisturbanceAge": {
        "PreDistAgeID": _get_constraints(primary_key=True),
        "SPUID": _get_constraints(index=True, foreign_key="tblSPU.SPUID"),
        "TimeStep": _get_constraints(index=True),
        "DistTypeID": {
            "args": [ForeignKey("tblDisturbanceType.DistTypeID")],
            "kwargs": {"index": True}
        },
        "UserDefdClassSetID": _get_constraints(
            index=True, foreign_key="tblClassifierSetValues.ClassifierSetID"),
        "LandClassID": _get_constraints(
            index=True, foreign_key="tblUNFCCCLandClass.UNFCCCLandClassID"),
        "kf2": _get_constraints(
            index=True, foreign_key="tblKP3334Flags.KP3334ID"),
        "kf3": _get_constraints(index=True),
        "kf4": _get_constraints(index=True),
        "kf5": _get_constraints(index=True),
        "kf6": _get_constraints(index=True)
    },
    "tblSPU": {
        "SPUID": _get_constraints(primary_key=True),
        "AdminBoundaryID": None,  # fk, index
        "EcoBoundaryID": None,  # fk, index
        "DefaultSPUID": None,  # fk, index
    },
    "tblSPUDefault": {
        "SPUID": _get_constraints(primary_key=True),
        "AdminBoundaryID": None,  # fk, index
        "EcoBoundaryID": None,  # fk, index
    },
    "tblSVL": {
        "SVLID": _get_constraints(primary_key=True),
        "SPUID": _get_constraints(index=True, foreign_key="tblSPU.SPUID"),
        "TimeStep": _get_constraints(index=True),
        "DistTypeID": {
            "args": [ForeignKey("tblDisturbanceType.DistTypeID")],
            "kwargs": {"index": True}
        },
        "UserDefdClassSetID": _get_constraints(
            index=True, foreign_key="tblClassifierSetValues.ClassifierSetID"),
        "LandClassID": _get_constraints(
            index=True, foreign_key="tblUNFCCCLandClass.UNFCCCLandClassID"),
        "kf2": _get_constraints(
            index=True, foreign_key="tblKP3334Flags.KP3334ID"),
        "kf3": _get_constraints(index=True),
        "kf4": _get_constraints(index=True),
        "kf5": _get_constraints(index=True),
        "kf6": _get_constraints(index=True)
    },
    "tblUNFCCCLandClass": {
        "UNFCCCLandClassID": _get_constraints(primary_key=True),
        "Name": None,  # unique, index
    }
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
    result = []
    for i_column, column in enumerate(df.columns):

        column_args = [
            column,
            _map_pandas_dtype(df.dtypes[i_column])]
        column_kwargs = {}

        column_constraint_args, column_constraint_kwargs = \
            _unpack_constraint_schema(constraint_defs, name, column)
        if column_constraint_args:
            column_args.extend(column_constraint_args)
        if column_constraint_kwargs:
            column_kwargs.update(column_constraint_kwargs)

        result.append(Column(*column_args, **column_kwargs))
    return result
