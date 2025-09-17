import pandas as pd
from pandas import json_normalize
import requests
import datetime
import time

from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
    
from dateutil.relativedelta import relativedelta
from pipeline.get_data_api import GetDataApi

from conectores.conectar_api_thunders import ConsultaThundersApi


class GetApi:
    
    def __init__(self):
        self.conn = ConsultaThundersApi()

    # # PROCESSO GERA LISTAGEM DE URLS CONFROME PARAMETROS - ESSA LISTA SERA PARTICIONADA EM LOTE PARA EXECUÇÃO EM PARALELO   
    # def generate_list_urls_juridic(self, p_url, p_list, p_uid):

    #     # Gerar a lista de URLs
    #     urls = []
    #     for item in p_list:
    #         # Cada item contém "workflowItemid" e "contracCode"
    #         workflow_item_id = item["workflowItemid"]
    #         contract_code = item["contracCode"]
            
    #         # print(workflow_item_id, contract_code)
    #         url = f"{p_url}{workflow_item_id}{p_uid}"
            
    #         # append url, workflow_item_id, contract_code
    #         urls.append((url, workflow_item_id, contract_code))

    #     print(f"Generated {len(urls)} URLs for API requests.")

    #     return urls
            
            
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
    
    # PROCESSO GERA DATAFRAME RETORNO DA API - BATE URL DE CADA LOTE - DEFINE ELEMNTO DE RETORNO DO JSON 
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
                        
            df = json_normalize(data_json)
            
            data_json_x = df.to_dict(orient="records")
            
            return data_json_x            
        
        except Exception as e:
            print(f"Error fetching data from {url} - {e}")
            return None

    # PROCESSO GERA DATAFRAME RETORNO DA API - BATE URL DE CADA LOTE , id, column
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


    # PROCESSO GERA DATAFRAME RETORNO DA API - BATE URL DE CADA LOTE , id, column
    def execute_requisicao_url_juridic(self, url, id, contractcode, column, headers):
        
        try:
            
            response = requests.request("GET", url, headers=headers)

            time.sleep(3)            

            response.raise_for_status()  # Lança erro se o status não for 200
            
            data_json = response.json()

            # INCLUSÃO DO ID NO JSON
            # data_json = [{**item, "currentWorkflowItemId": "id"} for item in data_json]

            df_ = json_normalize(data_json)
            
            df_['currentWorkflowItemId'] = id
            df_['currentContractCode'] = contractcode
                                    
            all_demands_data=[]
            # Iterate through each row in the DataFrame
            for index, row in df_.iterrows():
                if row[column]:  # Check if 'demands' column contains data
                    demands_list = row[column]

                    # Check if 'demands' is a list (and not NaN)
                    if isinstance(demands_list, list):
                        for demand in demands_list:
                            #print(demands)
                            # Add the original id and name to each demand row
                            # print(row['data'])
                            #demand['parent_guaranteedSavingsContractId'] = row['guaranteedSavingsContractId']
                            
                            # Append the demand to the list of all demand data
                            all_demands_data.append(demand) 
            

            dfx_ = pd.json_normalize(all_demands_data)

            df = dfx_.join(df_[['currentWorkflowStepId', 'currentWorkflowId', 'currentWorkflowItemId',
                'currentContractCode','currentWorkflowName', 'currentWorkflowIsActive',
                'currentStepBlockEditing', 'currentWorkflowCategoryId']], how="cross")            
                                        
            data_json_x = df.to_dict(orient="records")
            

            return data_json_x   
        
        except Exception as e:
            print(f"Error fetching data from {url} - {e}")
            return None
            
class ExecuteParalell:
    
    def __init__(self):
        self.conn = ConsultaThundersApi()
    
    # PROCESSO PARALELO COM DEFINIÇÃO DE ELEMENTO DO JSON
    def execute_processo_paralell_column(self, lists_urls, column):

        conn = ConsultaThundersApi()
        getapi = GetApi()
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
            with ThreadPoolExecutor(max_workers=20) as executor:
                
                future_to_url = [executor.submit(getapi.execute_requisicao_url_column, url, id, column, headers) for url, id, year in batch_urls] #url, headers, id, column) for url, id, year in batch_urls
                
                # wait(future_to_url)
                
                for future in as_completed(future_to_url):
                    df_results = future.result()
                    
                    if df_results:  # Apenas adiciona se não for None
                        results.append(df_results)
                
        df_final = pd.concat([pd.DataFrame(r) for r in results], ignore_index=True)

        # df_final.reset_index(drop=True, inplace=True)

        print("Data fetch and processing completed.")

        return df_final
    
    # PROCESSO PARALELO RETORNO DO JSON
    def execute_processo_paralell(self, lists_urls):

        conn = ConsultaThundersApi()
        getapi = GetApi()
        
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
            with ThreadPoolExecutor(max_workers=20) as executor:
                
                future_to_url = [executor.submit(getapi.execute_requisicao_url, url, id, headers) for url, id, year in batch_urls] #url, headers, id, column) for url, id, year in batch_urls
                
                # wait(future_to_url)
                
                for future in as_completed(future_to_url):
                    df_results = future.result()
                    
                    if df_results:  # Apenas adiciona se não for None
                        results.append(df_results)
                
        df_final = pd.concat([pd.DataFrame(r) for r in results], ignore_index=True)

        # df_final.reset_index(drop=True, inplace=True)

        print("Data fetch and processing completed.")

        return df_final


    # PROCESSO PARALELO COM DEFINIÇÃO DE ELEMENTO DO JSON
    def execute_processo_paralell_juridic(self, Thunders, lists_urls, column):

        conn = ConsultaThundersApi()
        getapi = GetApi()
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

            token = conn.get_token(Thunders)
        
            headers = {
                'authorization': 'Bearer '+ token
            }

            # print([(url, id) for url, id in batch_urls])
            # # print(batch_urls[1])
            # df_final= [getapi.execute_requisicao_url_node(url, id, column, headers) for url, id in batch_urls]
            
            # Paralelizar as requisições dentro do batch
            with ThreadPoolExecutor(max_workers=20) as executor:
                
                future_to_url = [executor.submit(getapi.execute_requisicao_url_juridic, url, workflowItemid, contractCode, column, headers) for url, workflowItemid, contractCode in batch_urls] #url, headers, id, column) for url, id, year in batch_urls
                
                # wait(future_to_url)
                
                for future in as_completed(future_to_url):
                    df_results = future.result()
                    
                    if df_results:  # Apenas adiciona se não for None
                        results.append(df_results)
                
        df_final = pd.concat([pd.DataFrame(r) for r in results], ignore_index=True)

        # df_final.reset_index(drop=True, inplace=True)

        print("Data fetch and processing completed.")

        return df_final