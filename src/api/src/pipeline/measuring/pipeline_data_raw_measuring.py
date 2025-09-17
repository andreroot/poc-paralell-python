from pipeline.get_data_api import GetDataApi
from pipeline.processing_data_paralell_v1 import GetApi, ExecuteParalell

from conectores.conectar_sql_server import ConectSqlServer
from conectores.conectar_api_thunders import ConsultaThundersApi

from datetime import datetime, date
from resources.utilitarios import Utils
from pandas import json_normalize

import pandas as pd

class GetDataMeasuring:
    
    def __init__(self):
        pass
    
    # função principal gera: 
    # df_MeasuringPoints | df_MeasuringPoint_details | df_api_MeasuringPoint_associatedAssets
    def run_getapi_MeasuringPoint(self):
        
        exec = GetDataApi()
        
        url = f"https://api.novo.thunders.com.br/gw/measurement/api/MeasuringPoints"
        
        data_json = exec.get_data_api( url)
        # list = [r['id'] for r in data_json]
        df_MeasuringPoints = pd.json_normalize(data_json)
        
        list_id = df_MeasuringPoints['id'].unique().tolist()
                                    
        # GET API COM PAARAMETRO DE LISTAGEM DE IDS
        df_MeasuringPoint_details = exec.get_data_api_param(list_id, url)

        # Create an empty list to hold all the expanded demand data
        all_demands_data = []
        
        # Iterate through each row in the DataFrame
        for index, row in df_MeasuringPoint_details.iterrows():
            if row['associatedAssets']:  # Check if 'demands' column contains data
                demands_list = row['associatedAssets']
                # print(type(demands_list))

                # Check if 'demands' is a list (and not NaN)
                if isinstance(demands_list, list):
                    # print(demands_list)
                    
                    for demand in demands_list:
                        #print(demands)
                        # Add the original id and name to each demand row
                        #print(row['associatedAssets'])
                        
                        demand['parent_id'] = row['id']
                        demand['parent_code'] = row['code']
                        demand['parent_name'] = row['name']
                        
                        # Append the demand to the list of all demand data
                        all_demands_data.append(demand)
        
                else:
                    print(f"No valid demands for ID: {row['associatedAssets']}")
            
            # Convert the list of demands to a new DataFrame
            if all_demands_data:
                df_api_MeasuringPoint_associatedAssets = json_normalize(all_demands_data)
            else:
                df_api_MeasuringPoint_associatedAssets = pd.DataFrame()  # Empty DataFrame if no demands data
                
        # Display the resulting DataFrame
        return df_MeasuringPoints, df_MeasuringPoint_details, df_api_MeasuringPoint_associatedAssets    

    # FUNÇÃO PARALELO que realiza as batidas em paralelo
    def get_api_MeasuringProjectionConsolidateMonthYear_paralell(self, df_MeasuringPoints):
                
        df = df_MeasuringPoints.copy()
                        
        url='https://api.novo.thunders.com.br/gw/measurement/api/MeasuringProjectionConsolidateMonthYear/'
        uid='/65975476-e71b-446f-a03a-155c58c86166/'
            
        # GERAR A LISTAGEM DE URLS
        getapi = GetApi()
        lists_urls = getapi.generate_list_urls(df, url, uid, 'MeasuringProjectionConsolidateMonthYear')

        # RETORNA UM ELEMENTO ESPECIFICO DO RETORNO DA API - ESPECIFICA COLUMN    
        exec = ExecuteParalell()
        result_concat = exec.execute_processo_paralell_column(lists_urls, 'periods')

        result_concat.reset_index(drop=True, inplace=True)
        # print("Data fetch and processing completed.")
        return result_concat

    # FUNÇÃO PARALELO que realiza as batidas em paralelo
    def get_api_MeasuringAdjustConsolidated_paralell(self, df_MeasuringPoints):
        
        df = df_MeasuringPoints.copy()

        url='https://api.novo.thunders.com.br/gw/measurement/api/MeasuringAdjustConsolidated/'
        uid='/'

        # GERAR A LISTAGEM DE URLS
        getapi = GetApi()
        lists_urls = getapi.generate_list_urls(df, url, uid, 'MeasuringAdjustConsolidated')
        
        # RETORNA UM ELEMENTO ESPECIFICO DO RETORNO DA API - ESPECIFICA COLUMN    
        exec = ExecuteParalell()
        result_concat = exec.execute_processo_paralell_column(lists_urls, 'months')
            
        result_concat.reset_index(drop=True, inplace=True)
        # print("Data fetch and processing completed.")
        return result_concat
    
    # FUNÇÃO PARALELO que realiza as batidas em paralelo
    def get_api_MeasuringAdjust_paralell(self, df_MeasuringPoints):

        df = df_MeasuringPoints.copy()

        url='https://api.novo.thunders.com.br/gw/measurement/api/MeasuringAdjust/'
        uid='/'
        
        # GERAR A LISTAGEM DE URLS
        getapi = GetApi()
        lists_urls = getapi.generate_list_urls(df, url, uid, 'MeasuringAdjust')

        # ENVIAR LISTAGEM DE URLS PARA OBTER RETORNO EM JSON 
        exec = ExecuteParalell()
        result_concat = exec.execute_processo_paralell(lists_urls)
        
        result_concat.reset_index(drop=True, inplace=True)
        # print("Data fetch and processing completed.")
        return result_concat


