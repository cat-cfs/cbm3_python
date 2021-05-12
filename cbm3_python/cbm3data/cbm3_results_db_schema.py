import sqlalchemy
from sqlalchemy import ForeignKey
from sqlalchemy import Column


CBM_RESULTS_CONSTRAINT_DEFS = {
    "tblAccountingRuleDiagnostics": {
        "DistTypeID": {
            "args": [ForeignKey("tblDisturbanceType.DistTypeID")]}
    },
    "tblAdminBoundary": {
        "AdminBoundaryID": {
            "kwargs": {"primary_key": True}
        },
        "AdminBoundaryName": {
            "kwargs": {"unique": True}
        },
        "DefaultAdminBoundaryID": {
            "args": [ForeignKey("tblAdminBoundaryDefault.AdminBoundaryID")]
        }
    },
    "tblAdminBoundaryDefault": {
        "AdminBoundaryID": {
            "kwargs": {"primary_key": True}
        },
        "AdminBoundaryName": {
            "kwargs": {"unique": True}
        }
    },

    "tblFluxIndicators": {
        "DistTypeID": {
            "args": [ForeignKey("tblDisturbanceType.DistTypeID")],
            "kwargs": {"index": True}
        },
        "SPUID": {
            "args": [ForeignKey("tblSPU.SPUID")],
            "kwargs": {"index": True}
        }
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
