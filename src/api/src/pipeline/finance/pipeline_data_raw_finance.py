from pipeline.get_data_api import GetDataApi

from conectores.conectar_sql_server import ConectSqlServer
from conectores.conectar_api_thunders import ConsultaThundersApi

from datetime import datetime, date
from resources.utilitarios import Utils
from pandas import json_normalize

import pandas as pd

class GetDataFinance:
    
    def __init__(self):
        pass
        
    def run_getapi_InvoiceExpirationType(self):

        exec = GetDataApi()
        
        url = "https://api.novo.thunders.com.br/gw/finance/api/FinanceTypes/InvoiceExpirationType"
        
        data_json = exec.get_data_api( url)
        
        df_InvoiceExpirationType = pd.DataFrame()
        
        df_InvoiceExpirationType = pd.json_normalize(data_json)
        
        #df_final = pd.concat([df_final, df], axis=0)
        
        df_InvoiceExpirationType.reset_index(drop=True, inplace=True)
            
        return df_InvoiceExpirationType  
    
    def run_getapi_paymentconditions(self):
        
        exec = GetDataApi()
        
        url = f"https://api.novo.thunders.com.br/gw/finance/api/paymentconditions"

        data_json = exec.get_data_api( url)
        
        df_paymentconditions = pd.DataFrame()
        
        df_paymentconditions = pd.json_normalize(data_json)
        
        #df_final = pd.concat([df_final, df], axis=0)
        
        df_paymentconditions.reset_index(drop=True, inplace=True)
                        
        df = df_paymentconditions.copy()

        # Create an empty list to hold all the expanded demand data
        all_demands_data = []
        
        # Iterate through each row in the DataFrame
        for index, row in df.iterrows():
            if row['data']:  # Check if 'demands' column contains data
                demands_list = row['data']
                #print(demands_list)
                
                # Check if 'demands' is a list (and not NaN)
                if isinstance(demands_list, list):
                    for demand in demands_list:
                        #print(demands)
                        # Add the original id and name to each demand row
                        # print(row['data'])
                        #demand['parent_guaranteedSavingsContractId'] = row['guaranteedSavingsContractId']
                        
                        # Append the demand to the list of all demand data
                        all_demands_data.append(demand)
        
                else:
                    print(f"No valid demands for ID: {row['data']}")
            
            # Convert the list of demands to a new DataFrame
            if all_demands_data:
                df_paymentconditions_data = json_normalize(all_demands_data)
            else:
                df_paymentconditions_data = pd.DataFrame()  # Empty DataFrame if no demands data
                
        return df_paymentconditions_data      

    def run_getapi_payments_incomes(self, thunders, cenario):

        exec = GetDataApi()
        
        url = f"https://api.novo.thunders.com.br/gw/finance/api/payments/incomes?&start=2023-01-01&end=2037-12-31&scenario={cenario}"

        data_json = exec.get_data_api_thunders(thunders, url)

        df = pd.DataFrame()
        
        df = pd.json_normalize(data_json)
        
        return df

    def run_getapi_payments_expenses(self, thunders, cenario):

        exec = GetDataApi()
        
        url =  f"https://api.novo.thunders.com.br/gw/finance/api/payments/expenses?&start=2023-01-01&end=2037-12-31&scenario={cenario}"

        data_json = exec.get_data_api_thunders(thunders, url)

        df = pd.DataFrame()
        
        df = pd.json_normalize(data_json)
        
        return df
    
    def run_getapi_payments(self):
        
        df_income_to_sql = self.run_getapi_payments_incomes()
        df_expenses_to_sql = self.run_getapi_payments_expenses()
        
        df_payments = pd.concat([df_income_to_sql, df_expenses_to_sql], ignore_index=True)
        
        return df_payments
    