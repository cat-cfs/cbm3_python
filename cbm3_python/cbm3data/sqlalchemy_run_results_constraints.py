import sqlalchemy


CONSTRAINTDEFS = {
    "tblFluxIndicators": {
        "DistTypeID": {
            "column_args": [sqlalchemy.ForeignKey("tblDisturbanceType.DistTypeID")],
            "column_kwargs": {"index": True}}
    }
}
