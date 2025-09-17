import pandas as pd
import requests
from dateutil.rrule import rrule, MONTHLY


from conectores.conectar_api_thunders import ConsultaThundersApi

class GetDataApi:
    
    def __init__(self):
        conn = ConsultaThundersApi()
        self.token = conn.get_token('BookComercial')
        
    def get_data_api(self, p_urls):
                       
        try:
            #print(f'Buscando Informações')
            
            url = p_urls
            
            headers = {
                'authorization': 'Bearer '+ self.token
            }
            response = requests.request("GET", url, headers=headers)
            
            data_json = response.json()
            
        except Exception as e:
            print(e)

        
        return data_json

    def get_data_api_param(self, list_id, p_url):
        
        df_final = pd.DataFrame()
        
        for id in list_id:
            
            try:
                # print(f'Buscando Informações do id: {id}')

                # montar url
                url = f"{p_url}/{id}"

                headers = {
                    'authorization': 'Bearer '+ self.token
                }

                response = requests.request("GET", url, headers=headers)

                if response.status_code==200:
                    data_json = response.json()
                    df = pd.json_normalize(data_json)
                
                df_final = pd.concat([df_final, df], axis=0)
                            
            except Exception as e:
                print(e)
        
        df_final.reset_index(drop=True, inplace=True)
        
        return df_final

    def get_data_api_param_date(self, start_date, end_date, p_url):
        
        # start_date = start_date
        # end_date  = end_date
        
        df_final = pd.DataFrame()
        
        for data in rrule(MONTHLY, dtstart=start_date, until=end_date):
            
            print(f'Buscando Informações da data: {data}') #+ str(data.strftime('%Y-%m-%d')))
            
            # url = "https://api.novo.thunders.com.br/gw/guaranteedsavings/GuaranteedSavingsContract/MonthView?showDeleted=false&PeriodReference="+ str(data.strftime('%Y-%m-%d')) + "&irt=true&is=true"

            p_data = str(data.strftime('%Y-%m-%d'))
            
            url = p_url.format(p_data=p_data)
            
            payload = ""
            
            headers = {
                'authorization': 'Bearer '+ self.token
            }
            
            response = requests.request("GET", url, data=payload, headers=headers)
            
            data_json = response.json()
            
            #print(data_json)
            
            df = pd.json_normalize(data=data_json)
            
            df['year'] = data.year
            df['month'] = data.month
            
            df_final = pd.concat([df_final, df], axis=0)
        
        df_final.reset_index(drop=True, inplace=True)
        
        return df_final
    
