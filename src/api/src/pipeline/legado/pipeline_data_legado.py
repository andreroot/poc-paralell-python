import time
import pandas as pd
import requests
import concurrent.futures
from dateutil.relativedelta import relativedelta
from datetime import datetime, date

from conectores.conectar_api_thunders import ConsultaThundersApi

def fetch_url_1(url, headers, id):
    try:
        response = requests.get(url, headers=headers)
        data_json = response.json()
        df_temp = pd.json_normalize(data_json)
        return df_temp


    except Exception as e:
        print(f"Error fetching data from {url} - {e}")
        return None

# Função principal para gerar URLs e fazer requisições em batches
def get_api_MeasuringAdjust(df):
    conn = ConsultaThundersApi()
    
    df_final = pd.DataFrame()
    
    # Gerar a lista de URLs
    urls = []

    start_date = datetime(2024,1, 1)
    end_date = datetime(2025, 1, 1)
    for index, row in df.iterrows():
        id = row['id']
        
        
        # Loop through each month from start_date to end_date
        current_date = start_date
        while current_date <= end_date:
            year = current_date.year
            month = f"{current_date.month:02d}"  # Format month as two digits
            url = f"https://api.novo.thunders.com.br/gw/measurement/api/MeasuringAdjust/{id}/{year}/{month}"
            urls.append((url, id, year))
            # Move to the next month
            current_date += relativedelta(months=1)

    print(f"Generated {len(urls)} URLs for API requests.")

    headers = {'authorization': 'Bearer ' + conn.get_token('BookComercial')}

    # Lotes de URLs (batchs) com até 400 requisições
    batch_size = 400
    num_batches = (len(urls) // batch_size) + 1

    for batch_num in range(num_batches):
        start_index = batch_num * batch_size
        end_index = min(start_index + batch_size, len(urls))
        batch_urls = urls[start_index:end_index]

        print(f"Processing batch {batch_num + 1}/{num_batches} with {len(batch_urls)} requests.")

        # Paralelizar as requisições dentro do batch
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(fetch_url_1, url, headers,id) for url, id, year in batch_urls]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result is not None:
                    df_final = pd.concat([df_final, result], axis=0)

        # Pausar por 60 segundos entre os lotes
        if batch_num < num_batches - 1:
            print("Waiting 60 seconds before the next batch...")
            time.sleep(60)

    df_final.reset_index(drop=True, inplace=True)
    print("Data fetch and processing completed.")
    return df_final

# Função para fazer a requisição da API com rate limit
# Função para fazer a requisição da API
def fetch_url(url, headers,id):
    try:
        response = requests.get(url, headers=headers)
        data_json = response.json()

        # Verifica se há dados em 'months'
        if 'months' in data_json and data_json['months']:
            df_temp = pd.json_normalize(data_json['months'])
            df_temp['id'] = id  # Adiciona a coluna 'id'
            return df_temp
        else:
            return None

    except Exception as e:
        print(f"Error fetching data from {url} - {e}")
        return None

# Função principal para gerar URLs e fazer requisições em batches
def get_api_MeasuringAdjustConsolidated2(df):
    
    conn = ConsultaThundersApi()
    
    df_final = pd.DataFrame()

    # Extrair o ano da coluna 'startDate'
    df['year'] = pd.to_datetime(df['startDate']).dt.year

    # Gerar a lista de URLs
    urls = []
    for index, row in df.iterrows():
        id = row['id']
        start_year = row['year']
        
        # Gerar URLs para os anos de start_year até 2035
        for year in range(start_year, 2035):
            url = f"https://api.novo.thunders.com.br/gw/measurement/api/MeasuringAdjustConsolidated/{id}/{year}"
            urls.append((url, id, year))

    print(f"Generated {len(urls)} URLs for API requests.")

    headers = {'authorization': 'Bearer ' +  conn.get_token('BookComercial')}

    # Lotes de URLs (batchs) com até 400 requisições
    batch_size = 400
    num_batches = (len(urls) // batch_size) + 1

    for batch_num in range(num_batches):
        start_index = batch_num * batch_size
        end_index = min(start_index + batch_size, len(urls))
        batch_urls = urls[start_index:end_index]

        print(f"Processing batch {batch_num + 1}/{num_batches} with {len(batch_urls)} requests.")

        # Paralelizar as requisições dentro do batch
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(fetch_url, url, headers,id) for url, id, year in batch_urls]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result is not None:
                    df_final = pd.concat([df_final, result], axis=0)

        # Pausar por 60 segundos entre os lotes
        if batch_num < num_batches - 1:
            print("Waiting 60 seconds before the next batch...")
            time.sleep(60)

    df_final.reset_index(drop=True, inplace=True)
    print("Data fetch and processing completed.")
    return df_final

