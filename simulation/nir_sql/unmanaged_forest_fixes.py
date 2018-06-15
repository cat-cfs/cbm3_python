from cbm3data.accessdb import AccessDB
import logging

def run_uf_results_fixes(uf_rrdb_path):


    sql =[
        "CREATE TABLE tblFixLandClasses (FromClass Int, OldToClass Int, NewToClass Int);",
        "INSERT INTO tblFixLandClasses (FromClass,OldToClass,NewToClass) VALUES (16,13,19);",
        "INSERT INTO tblFixLandClasses (FromClass,OldToClass,NewToClass) VALUES (16,14,20);",
        "INSERT INTO tblFixLandClasses (FromClass,OldToClass,NewToClass) VALUES (19,0,3);",
        "INSERT INTO tblFixLandClasses (FromClass,OldToClass,NewToClass) VALUES (20,0,4);",

        """UPDATE tblAgeIndicators 
        INNER JOIN tblFixLandClasses 
        ON tblAgeIndicators.LandClassID = tblFixLandClasses.OldToClass 
        SET tblAgeIndicators.LandClassID = tblFixLandClasses.NewToClass;""",

        """UPDATE tblDistIndicators INNER JOIN tblFixLandClasses 
        ON tblDistIndicators.LandClassID = tblFixLandClasses.OldToClass 
        SET tblDistIndicators.LandClassID = tblFixLandClasses.NewToClass;""",

        """UPDATE tblFluxIndicators INNER JOIN tblFixLandClasses 
        ON tblFluxIndicators.LandClassID = tblFixLandClasses.OldToClass 
        SET tblFluxIndicators.LandClassID = tblFixLandClasses.NewToClass;""",

        """UPDATE tblNIRSPecialOutput INNER JOIN tblFixLandClasses 
        ON tblNIRSPecialOutput.LandClass_From=tblFixLandClasses.OldToClass 
        SET tblNIRSPecialOutput.LandClass_From = NewToClass
        WHERE tblNIRSPecialOutput.LandClass_From In (13,14);""",

        """UPDATE tblNIRSpecialOutput INNER JOIN tblFixLandClasses 
        ON (tblNIRSpecialOutput.LandClass_To=tblFixLandClasses.OldToClass) 
        AND (tblNIRSpecialOutput.LandClass_From=tblFixLandClasses.FromClass) 
        SET tblNIRSpecialOutput.LandClass_To = tblFixLandClasses.NewToClass;""",

        """
        UPDATE tblPoolIndicators INNER JOIN tblFixLandClasses 
        ON tblPoolIndicators.LandClassID = tblFixLandClasses.OldToClass 
        SET tblPoolIndicators.LandClassID = tblFixLandClasses.NewToClass;""",

        """
        UPDATE tblPreDistAge INNER JOIN tblFixLandClasses 
        ON tblPreDistAge.LandClassID = tblFixLandClasses.OldToClass 
        SET tblPreDistAge.LandClassID = tblFixLandClasses.NewToClass;"""
    ]

    with AccessDB(uf_rrdb_path) as rrdb:
        for q in sql:
            rrdb.ExecuteQuery(q)




