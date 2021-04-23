import sqlalchemy
from sqlalchemy import ForeignKey
from sqlalchemy import Column


CBM_RESULTS_CONSTRAINT_DEFS = {
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
    if dtype == "int64":
        return sqlalchemy.Integer
    elif dtype == "float64":
        return sqlalchemy.Float
    elif dtype == "object":
        return sqlalchemy.String
    elif dtype == "bool":
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
            if "column_args" in column_defs:
                args = column_defs["column_args"]
            if "column_kwargs" in column_defs:
                kwargs = column_defs["column_kwargs"]
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
