import pandas as pd
from pandas import json_normalize
import requests
import datetime
import time

from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
    
from dateutil.relativedelta import relativedelta
from pipeline.get_data_api import GetDataApi

from conectores.conectar_api_thunders import ConsultaThundersApi


class ExecuteParalell:
    
    def __init__(self):
        self.conn = ConsultaThundersApi()
    
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
    def execute_requisicao_url_column(self, url, id, column, headers):
        
        try:
            
            # exec = GetDataApi()
            
            # # GET API COM PAARAMETRO DE LISTAGEM DE IDS
            # data_json = exec.get_data_api(url, headers)

            response = requests.request("GET", url, headers=headers)
            
            time.sleep(3)         
            
            response.raise_for_status()  # Lança erro se o status não for 200
            
            data_json = response.json()
            
            data_json = data_json[column]
            
            # INCLUSÃO DO ID NO JSON
            data_json = [{**item, "id": str(id)} for item in data_json]
            
            return data_json            
        
        except Exception as e:
            print(f"Error fetching data from {url} - {e}")
            return None

    # PROCESSO GERA DATAFRAME RETIORNO DA APT - BATE URL DE CADA LOTE , id, column
    def execute_requisicao_url(self, url, id, headers):
        
        try:
            
            response = requests.request("GET", url, headers=headers)

            time.sleep(3)            

            response.raise_for_status()  # Lança erro se o status não for 200
            
            data_json = response.json()
            
            # INCLUSÃO DO ID NO JSON
            data_json = [{**item, "id": str(id)} for item in data_json]
            
            return data_json
        
        except Exception as e:
            print(f"Error fetching data from {url} - {e}")
            return None
    
    # PROCESSO PARALELO 
    def execute_processo_paralell_column(self, lists_urls, column):

        conn = ConsultaThundersApi()
        # Lotes de URLs (batchs) com até 400 requisições
        batch_size = 400
        # arredonda para um numero inteiro
        num_batches = (len(lists_urls) // batch_size) + 1

        df_final = pd.DataFrame()
        results=[]
        
        for batch_num in range(num_batches):
            
            # apartir da execução atual multiplica pelo lote(1*500 2*500)
            start_index = batch_num * batch_size
            print(start_index)

            # ate a ultima partição desejada para particionar a execução, o minino dos valors inicial+lote
            # faz sentido na ultima execução que constroe a partição para execução do restante de dados                   
            end_index = min(start_index + batch_size, len(lists_urls))
            print(end_index)
            
            # lote filtrado pelo tamanho do batch calculado           
            batch_urls = lists_urls[start_index:end_index]

            print(f"Processing batch {batch_num + 1}/{num_batches} with {len(batch_urls)} requests.")

            token = conn.get_token('BookComercial')
        
            headers = {
                'authorization': 'Bearer '+ token
            }
        
            # Paralelizar as requisições dentro do batch
            # with ThreadPoolExecutor(max_workers=10) as executor:
            #     futures = [executor.submit(self.execute_req_get_data, url, headers, id, column) for url, id, year in batch_urls]
            #     for future in as_completed(futures):
            #         result = future.result()
            #         if result is not None:
            #             df_final = pd.concat([df_final, result], axis=0)

            # Paralelizar as requisições dentro do batch
            with ThreadPoolExecutor(max_workers=40) as executor:
                
                future_to_url = [executor.submit(self.execute_requisicao_url, url, id, column, headers) for url, id, year in batch_urls] #url, headers, id, column) for url, id, year in batch_urls
                
                # wait(future_to_url)
                
                for future in as_completed(future_to_url):
                    df_results = future.result()
                    
                    if df_results:  # Apenas adiciona se não for None
                        results.append(df_results)
                
        df_final = pd.concat([pd.DataFrame(r) for r in results], ignore_index=True)

        # df_final.reset_index(drop=True, inplace=True)

        print("Data fetch and processing completed.")

        return df_final
    
    # PROCESSO PARALELO 
    def execute_processo_paralell(self, lists_urls):

        conn = ConsultaThundersApi()
        # Lotes de URLs (batchs) com até 400 requisições
        batch_size = 400
        # arredonda para um numero inteiro
        num_batches = (len(lists_urls) // batch_size) + 1

        df_final = pd.DataFrame()
        results=[]
        
        for batch_num in range(num_batches):
            
            # apartir da execução atual multiplica pelo lote(1*500 2*500)
            start_index = batch_num * batch_size
            print(start_index)

            # ate a ultima partição desejada para particionar a execução, o minino dos valors inicial+lote
            # faz sentido na ultima execução que constroe a partição para execução do restante de dados                   
            end_index = min(start_index + batch_size, len(lists_urls))
            print(end_index)
            
            # lote filtrado pelo tamanho do batch calculado           
            batch_urls = lists_urls[start_index:end_index]

            print(f"Processing batch {batch_num + 1}/{num_batches} with {len(batch_urls)} requests.")

            token = conn.get_token('BookComercial')
        
            headers = {
                'authorization': 'Bearer '+ token
            }
        
            # Paralelizar as requisições dentro do batch
            # with ThreadPoolExecutor(max_workers=10) as executor:
            #     futures = [executor.submit(self.execute_req_get_data, url, headers, id, column) for url, id, year in batch_urls]
            #     for future in as_completed(futures):
            #         result = future.result()
            #         if result is not None:
            #             df_final = pd.concat([df_final, result], axis=0)

            # Paralelizar as requisições dentro do batch
            with ThreadPoolExecutor(max_workers=5) as executor:
                
                future_to_url = [executor.submit(self.execute_requisicao_url, url, id, headers) for url, id, year in batch_urls] #url, headers, id, column) for url, id, year in batch_urls
                
                # wait(future_to_url)
                
                for future in as_completed(future_to_url):
                    df_results = future.result()
                    
                    if df_results:  # Apenas adiciona se não for None
                        results.append(df_results)
                
        df_final = pd.concat([pd.DataFrame(r) for r in results], ignore_index=True)

        # df_final.reset_index(drop=True, inplace=True)

        print("Data fetch and processing completed.")

        return df_final

