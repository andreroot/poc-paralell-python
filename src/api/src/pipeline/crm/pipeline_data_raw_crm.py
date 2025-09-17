from pipeline.get_data_api import GetDataApi

from conectores.conectar_sql_server import ConectSqlServer
from conectores.conectar_api_thunders import ConsultaThundersApi

from datetime import datetime, date
from resources.utilitarios import Utils
from pandas import json_normalize

import pandas as pd

class GetDataCRM:
    
    def __init__(self):
        pass
        
    def run_getapi_loadPhysicalAssets(self): 
        
        exec = GetDataApi()
        
        url="https://api.novo.thunders.com.br/gw/crm/api/loadPhysicalAssets" 
        
        # pegar id 
        data_json = exec.get_data_api(url)
        
        list = [r['id'] for r in data_json]

        # GET API COM PAARAMETRO DE LISTAGEM DE IDS
        df_loadPhysicalAssets = exec.get_data_api_param(list, url)
        
        df_loadPhysicalAssets_details = df_loadPhysicalAssets.copy()
        df_demands_ = df_loadPhysicalAssets.copy()

        ## PROCESSING - LOAD PHYSICALASSESTS
        
        # Create an empty list to hold all the expanded demand data
        all_demands_data = []

        # Iterate through each row in the DataFrame
        for index, row in df_demands_.iterrows():
            if row['demands']:  # Check if 'demands' column contains data
                demands_list = row['demands']
                
                # If it's already a list, process it
                for demand in demands_list:
                    # Add the original id and name to each demand row
                    # print(row['id'],row['name'])
                    demand['parent_id'] = row['id']
                    demand['parent_name'] = row['name']
                    
                    # Append the demand to the list of all demand data
                    all_demands_data.append(demand)

        # Convert the list of demands to a new DataFrame
        df_demands = json_normalize(all_demands_data)
            
        return df_loadPhysicalAssets_details, df_demands
