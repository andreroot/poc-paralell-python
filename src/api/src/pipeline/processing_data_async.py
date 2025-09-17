
import asyncio
import aiohttp
import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

from conectores.conectar_api_thunders import ConsultaThundersApi

class ExecuteAsyncio:
    
    def __init__(self):
        self.conn = ConsultaThundersApi()
    
    async def fetch(self, session, url, headers, id):
        async with session.get(url, headers=headers) as response:
            # response = requests.request("GET", url, headers=headers)
            # time.sleep(5)            
            data_json = await response.json()
            return data_json, id


    async def execute_req_get_data_async(self, list_urls, headers):
        async with aiohttp.ClientSession() as session:
            tasks = [asyncio.ensure_future(self.fetch(session, url, headers, id)) for url, id in list_urls]
            responses = await asyncio.gather(*tasks)

            # responses = await asyncio.gather(*[self.fetch(session, url, headers, id) for url, id in list_urls])
                        
            for response in responses:
                data_json = response.json()
                return data_json
            


    # PROCESSO GERA LISTAGEM DE URLS CONFROME PARAMETROS - ESSA LISTA SERA PARTICIONADA EM LOTE PARA EXECUÇÃO EM PARALELO   
    def generate_list_urls(self, df, p_url, p_uid, tab):
        
        df_final = pd.DataFrame()

        # Extrair o ano da coluna 'startDate'
        # df['year'] = df.apply(lambda x: pd.to_datetime(x['startDate']).dt.year, axis=1)
        df['year'] = df.apply(lambda x: pd.to_datetime(x['startDate']).strftime("%Y"), axis=1)

        # Gerar a lista de URLs
        urls = []
        for index, row in df.iterrows():
            id = row['id']
            
            
            if tab=='MeasuringAdjust':
                start_date = datetime.datetime(2024, 1, 1)
                end_date = datetime.datetime(2025, 1, 1)
                    
                # Loop through each month from start_date to end_date
                current_date = start_date
                while current_date <= end_date:
                    # print(current_date)
                    
                    year = current_date.year
                    
                    month = f"{current_date.month:02d}"  # Format month as two digits
                    
                    url = f"{p_url}{id}{p_uid}{year}{p_uid}{month}"
                    
                    urls.append((url, id, year))
                    # Move to the next month
                    
                    current_date += relativedelta(months=1)
                        
            else:
            
                start_year = row['year']
                # Gerar URLs para os anos de start_year até 2035
                for year in range(int(start_year), 2035):
                    
                    url = f"{p_url}{id}{p_uid}{year}"
                    
                    urls.append((url, id, year))

        print(f"Generated {len(urls)} URLs for API requests.")
        
        return urls
    
    # PROCESSO GERA DATAFRAME RETIORNO DA APT - BATE URL DE CADA LOTE 
    def execute_req_get_data(self, list_urls, headers, column):
        
        df_temp = pd.DataFrame()
        try:
            
            data_json = asyncio.run(self.execute_req_get_data_async(list_urls, headers))
                        
            if column in data_json and data_json[column]:
                
                df_temp = pd.json_normalize(data_json[column])
            
            elif column==None:
                
                 df_temp = pd.json_normalize(data_json)
            
            else:
                return df_temp
            
            df_temp['id'] = id  # Adiciona a coluna 'id'
            
            return df_temp
        
        except Exception as e:
            print(f"Error fetching data from {url} - {e}")
            return df_temp            