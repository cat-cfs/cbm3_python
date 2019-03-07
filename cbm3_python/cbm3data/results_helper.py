import pandas as pd
from cbm3_python.cbm3data.accessdb import AccessDB


def get_classifier_values(results_path):
    '''
    loads the classifier values in the specified results database into an
    indexed collection to serve for labels, grouping and filtering CBM results tables
    '''
    sql= """
    SELECT
    tblUserDefdClassSetValues.UserDefdClassSetID,
    tblUserDefdClasses.ClassDesc,
    tblUserDefdSubclasses.UserDefdSubClassName
    FROM ( 
        tblUserDefdClasses INNER JOIN tblUserDefdClassSetValues ON
        tblUserDefdClasses.UserDefdClassID = tblUserDefdClassSetValues.UserDefdClassID
    ) INNER JOIN tblUserDefdSubclasses ON (
        tblUserDefdClassSetValues.UserDefdSubclassID = tblUserDefdSubclasses.UserDefdSubclassID
    ) AND (
        tblUserDefdClassSetValues.UserDefdClassID = tblUserDefdSubclasses.UserDefdClassID);
    """
    result = {}
    with AccessDB(results_path) as rrdb:
        for row in rrdb.Query(sql):
            classifier_set_id = row["UserDefdClassSetID"]
            if classifier_set_id in result:
                result[classifier_set_id].append({
                    row["ClassDesc"]:
                    row["UserDefdSubClassName"]})
    return result


def pool_indicator_column_meta():
    return [
        {"index": 0, "column_name": "PoolIndID"},
        {"index": 1, "column_name": "TimeStep"},
        {"index": 2, "column_name": "SPUID"},
        {"index": 3, "column_name": "UserDefdClassSetID"},
        {"index": 4, "column_name": "VFastAG", "name": "Aboveground Very Fast DOM", "tags": ["Total Ecosystem","Dead Organic Matter", "Aboveground DOM", "Litter"]},
        {"index": 5, "column_name": "VFastBG", "name": "Belowground Very Fast DOM", "tags": ["Total Ecosystem", "Dead Organic Matter", "Belowground DOM"]},
        {"index": 6, "column_name": "FastAG", "name": "Aboveground Fast DOM", "tags": ["Total Ecosystem", "Dead Organic Matter", "Aboveground DOM", "Litter"]},
        {"index": 7, "column_name": "FastBG", "name": "Belowground Fast DOM", "tags": ["Total Ecosystem", "Dead Organic Matter", "Belowground DOM", "Deadwood"]},
        {"index": 8, "column_name": "Medium", "name": "Medium DOM", "tags": ["Total Ecosystem", "Dead Organic Matter", "Aboveground DOM", "Deadwood"]},
        {"index": 9, "column_name": "SlowAG", "name": "Aboveground Slow DOM", "tags": ["Total Ecosystem", "Dead Organic Matter", "Aboveground DOM", "Litter"]},
        {"index": 10, "column_name": "SlowBG", "name": "Belowground Slow DOM", "tags": ["Total Ecosystem", "Dead Organic Matter", "Belowground DOM"]},
        {"index": 11, "column_name": "SWStemSnag", "name": "Softwood Stem Snag", "tags": ["Total Ecosystem", "Dead Organic Matter", "Aboveground DOM", "Deadwood"]},
        {"index": 12, "column_name": "SWBranchSnag", "name": "Softwood Branch Snag", "tags": ["Total Ecosystem", "Dead Organic Matter", "Aboveground DOM", "Deadwood"]},
        {"index": 13, "column_name": "HWStemSnag", "name": "Hardwood Stem Snag", "tags": ["Total Ecosystem", "Dead Organic Matter", "Aboveground DOM", "Deadwood"]},
        {"index": 14, "column_name": "HWBranchSnag", "name": "Hardwood Branch Snag", "tags": ["Total Ecosystem", "Dead Organic Matter", "Aboveground DOM", "Deadwood"]},
        {"index": 15, "column_name": "BlackCarbon", "name": "Total Ecosystem", "tags": ["Dead Organic Matter"]},
        {"index": 16, "column_name": "Peat", "name": "Total Ecosystem", "tags": ["Dead Organic Matter"]},
        {"index": 17, "column_name": "LandClassID"},
        {"index": 18, "column_name": "kf2"},
        {"index": 19, "column_name": "kf3"},
        {"index": 20, "column_name": "kf4"},
        {"index": 21, "column_name": "kf5"},
        {"index": 22, "column_name": "kf6"},
        {"index": 23, "column_name": "SW_Merch", "name": "Softwood Merchantable", "tags": ["Total Ecosystem", "Total Biomass", "Softwood", "Aboveground Biomass"]},
        {"index": 24, "column_name": "SW_Foliage", "name": "Softwood Foliage", "tags": ["Total Ecosystem", "Total Biomass", "Softwood", "Aboveground Biomass"]},
        {"index": 25, "column_name": "SW_Other", "name": "Softwood Other", "tags": ["Total Ecosystem", "Total Biomass", "Softwood", "Aboveground Biomass"]},
        {"index": 26, "column_name": "SW_subMerch", "name": "Total Ecosystem", "tags": ["Total Biomass", "Softwood", "Aboveground Biomass"]},
        {"index": 27, "column_name": "SW_Coarse", "name": "Softwood Coarse Roots", "tags": ["Total Ecosystem", "Total Biomass", "Softwood", "BelowGroundBiomass"]},
        {"index": 28, "column_name": "SW_Fine", "name": "Softwood Fine Roots", "tags": ["Total Ecosystem", "Total Biomass", "Softwood", "BelowGroundBiomass"]},
        {"index": 29, "column_name": "HW_Merch", "name": "Hardwood Merchantable", "tags": ["Total Ecosystem", "Total Biomass", "Hardwood", "Aboveground Biomass"]},
        {"index": 30, "column_name": "HW_Foliage", "name": "Hardwood Foliage", "tags": ["Total Ecosystem", "Total Biomass", "Hardwood", "Aboveground Biomass"]},
        {"index": 31, "column_name": "HW_Other", "name": "Total Ecosystem", "tags": ["Total Biomass", "Hardwood", "Aboveground Biomass"]},
        {"index": 32, "column_name": "HW_subMerch", "name": "Hardwood Other", "tags": ["Total Ecosystem", "Total Biomass", "Hardwood", "Aboveground Biomass"]},
        {"index": 33, "column_name": "HW_Coarse", "name": "Hardwood Coarse Roots", "tags": ["Total Ecosystem", "Total Biomass", "Hardwood", "BelowGroundBiomass"]},
        {"index": 34, "column_name": "HW_Fine", "name": "Hardwood Find Roots", "tags": ["Total Ecosystem", "Total Biomass", "Hardwood", "BelowGroundBiomass"]}
        ]

def as_data_frame(query, results_db_path):
    with AccessDB(results_db_path) as results_db:
        df = pd.read_sql(query, results_db.connection)
