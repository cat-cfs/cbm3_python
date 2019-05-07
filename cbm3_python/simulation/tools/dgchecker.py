'''
Created on Apr 26, 2019

@author: gazhang
'''

import pandas as pd
import xml.etree.ElementTree as ET
from pathlib import Path
from cbm3_python.cbm3data.accessdb import AccessDB
 
class DGChecker(object):

    def __init__(self, dg_default_path, tasks):
        self.tasks = tasks
        self.event_queries = self.__load_dg_metadata(dg_default_path)
        
        #query the event database to get alll of the event data
        self.__query_source_disturbance_data()
        
    def __load_dg_metadata(self, xml_path):
        tree = ET.parse(xml_path)
        root = tree.getroot()
        result = []
        
        for d in root[0].findall("DGDatasource"):
            dist_class = d.attrib["DisturbanceClass"]
            
            for q in d.find("Queries").findall("Query"):
                name = q.attrib["Name"]
                query = q.find("SQL").text
                
            result.append({"dist_class": dist_class, "query": query})
        
        return result
      
    def __query_source_disturbance_data (self):
        for task in self.tasks:
            dist_class = task["DisturbanceClass"]
            disturbance_DBPath = task["DisturbanceDBPath"]
            
            query = self.__get_query_by_distclass(dist_class)
            
            #record the event start year to be used to query project events
            self.event_start_year = query[-4:]
            
            #for each task, query the event database
            with AccessDB(disturbance_DBPath) as event_db:
                if dist_class == "ForestManagement":
                    self.fm_df = pd.read_sql(query, event_db.connection)
                elif dist_class == "Bioenergy":
                    self.bio_df = pd.read_sql(query, event_db.connection)


    def __get_query_by_distclass(self, dist_class):
        for item in self.event_queries:
            if item["dist_class"] == dist_class:
                return item["query"]


    def __get_event_data(self, dist_class, project_prefix):
        if dist_class == "ForestManagement":
            df_events = self.fm_df.loc[self.fm_df["ProjectName"] == project_prefix]
        elif dist_class == "Bioenergy":
            df_events = self.bio_df.loc[self.bio_df["ProjectName"] == project_prefix]
        return df_events 


    def __get_project_default_disturbance_ids(self, dist_class, project_prefix):
        df_events = self.__get_event_data(dist_class, project_prefix)
        unique_default_dist = df_events.DefaultDistType.unique()
        return unique_default_dist


    def __build_project_events_query_string(self, project_DBPath, default_disturbances):
        qryStr = """
            select 
                tDT.DefaultDistTypeID as DefaultDistType,
                tDT.DistTypeName,
                (tDE.TimeStepStart + 1989) as HarvestYear,
                tDT.DistTypeID,
                IIF(tDE.DistFormat = 1, 'DistArea', IIF(tDE.DistFormat = 2, 'MerchCarbonToDisturb', '')) as TargetType,
                sum(IIF(tDE.DistFormat = 1, tDE.DistArea, tDE.MerchCarbonToDisturb)) AS ProjectTarget
            from tblDisturbanceType as tDT inner join
                (tblDisturbanceEvents as tDE inner join
                    (tblDisturbanceGroupScenario as tDGS inner join
                        (
                        select distinct tRDSL.DisturbanceGroupScenarioID, tRDSL.RunDisturbanceScenarioID, tRDSL.DistTypeID, tRTD.RunLength
                        from tblRunDisturbanceScenarioLookup as tRDSL inner join
                          (tblRunTable as tRT inner join tblRunTableDetails as tRTD on tRT.RunID = tRTD.RunID)
                        on tRT.RunDisturbanceScenarioID = tRDSL.RunDisturbanceScenarioID
                        in '{0}'
                        where tRT.RunID = (select max(runid) from tblSimulation in '{0}')
                        ) as join1
                        on tDGS.DisturbanceGroupScenarioID = join1.DisturbanceGroupScenarioID
                    )
                on tDE.DisturbanceGroupScenarioID = tDGS.DisturbanceGroupScenarioID
            )
            on tDT.DistTypeID = tDGS.DistTypeID
            in '{0}'
            where tDT.DefaultDistTypeID in ({1}) and
                tDE.TimeStepStart <= join1.RunLength  and
                tDE.TimeStepStart + 1989 >= {2}
            group by
                tDT.DefaultDistTypeID,
                tDT.DistTypeID,
                tDT.DistTypeName,
                tDE.TimeStepStart,
                tDE.DistFormat
            order by
                tDT.DefaultDistTypeID, tDE.TimeStepStart
        """.format(project_DBPath, default_disturbances, self.event_start_year)

        return qryStr


    def create_check_df(self, dist_class, project_prefix, project_path):
        default_disturbance_types = self.__get_project_default_disturbance_ids(dist_class, project_prefix)
        if len(default_disturbance_types) == 0:
            #if the disturbance query returns no disturbance types for
            #the specified project prefix, we can safely say the
            #disturbance generator did not export events to the project
            return None
        str_default_distTypes =", " .join(str(id) for id in default_disturbance_types)

        df_events = self.__get_event_data(dist_class, project_prefix)
        df_events = df_events.groupby(['ProjectName','DefaultDistType','TargetType', 'HarvestYear']).agg({"ProjectTarget": "sum"})

        project_query = self.__build_project_events_query_string(project_path, str_default_distTypes)
        with AccessDB(project_path) as project_db:
            df_proj = pd.read_sql(project_query, project_db.connection)

        df_check = pd.merge(df_events, df_proj, how='left', on=['DefaultDistType', 'HarvestYear'])
        df_check['difference'] = df_check['ProjectTarget_x'] - df_check['ProjectTarget_y']
        df_check['relative_difference'] = df_check['difference'].abs() / \
            ((df_check['ProjectTarget_x'] + df_check['ProjectTarget_y']) / 2.0)
        df_check['dist_class'] = dist_class
        return df_check

    def check(self, project_prefix, project_path):
        #create qaqc log file
        df_check= pd.DataFrame()
        for task in self.tasks:
            df_check = df_check.append(
                self.create_check_df(
                    task["DisturbanceClass"],
                    project_prefix, project_path))
        if df_check.shape[0]==0:
            #don't write a file with an empty dataframe
            return
        project_dir = Path(project_path).parent
        out_path = Path.joinpath(project_dir, "{0}_disturbance_generator_qaqc.csv".format(project_prefix))
        with open(out_path, 'w', newline='') as output:
            df_check.to_csv(output)
        if df_check['relative_difference'].max() > 5e-8: 
            #since the project db format has 32bit precision
            #on some of the columns we are checking, we need
            # to tolerate a somewhat large difference
            raise ValueError("large difference detected in disturbance generator output")