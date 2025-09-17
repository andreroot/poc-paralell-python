from pipeline.get_data_api import GetDataApi
# from pipeline.processing_data_paralell import ExecuteParalell

# from conectores.conectar_sql_server import ConectSqlServer
# from conectores.conectar_api_thunders import ConsultaThundersApi

from datetime import datetime, date
from resources.utilitarios import Utils
from pandas import json_normalize

import pandas as pd

class GetDataGuaranted:
    
    def __init__(self):
        pass
    
    # função principal gera: 
    # df_MonthView | df_guaranteed_savings_charges |  df_guaranteed_savings_charges_services
    def run_getapi_GuaranteedSavingsContract(self): 
            
        exec = GetDataApi()
        
        url = "https://api.novo.thunders.com.br/gw/guaranteedsavings/GuaranteedSavingsContract/MonthView?showDeleted=false&PeriodReference={p_data}&irt=true&is=true"
        
        start_date = date(2024, 1, 1) 
        end_date = date(2035, 12, 1) 

        df_MonthView = exec.get_data_api_param_date(start_date, end_date, url)
        
        df_MonthView['startPeriod'] = pd.to_datetime(df_MonthView['startPeriod'])
        df_MonthView['endPeriod'] = pd.to_datetime(df_MonthView['endPeriod'])
        df_MonthView['createdDate'] = pd.to_datetime(df_MonthView['createdDate']).dt.tz_convert(None)
        df_MonthView['contractPeriodStart'] = pd.to_datetime(df_MonthView['contractPeriodStart'])
        df_MonthView['contractPeriodEnd'] = pd.to_datetime(df_MonthView['contractPeriodEnd'])
        df_MonthView['lastUpdate'] = pd.to_datetime(df_MonthView['lastUpdate']).dt.tz_convert(None)
        
        url = f"https://api.novo.thunders.com.br/gw/guaranteedsavings/GuaranteedSavingsContract"
        
        list = df_MonthView['id'].unique().tolist()

        # enviar lista de ids para api - do datframe df_MonthView
        df_guaranteed_savings_charges = exec.get_data_api_param(list, url)

        ## PROCESSING - GUARANTEE SAVING CONTRACT -> GUARANTEE SAVING CONTRACT SERVICES

        # Create an empty list to hold all the expanded demand data
        all_demands_data = []
        
        # Iterate through each row in the DataFrame
        for index, row in df_guaranteed_savings_charges.iterrows():
            if row['servicePeriods']:  # Check if 'demands' column contains data
                demands_list = row['servicePeriods']
                
                # If it's already a list, process it
                for demand in demands_list:
                    # Add the original id and name to each demand row
                    print(row['id'],row['code'])
                    demand['parent_id'] = row['id']
                    demand['parent_code'] = row['code']
                    
                    # Append the demand to the list of all demand data
                    all_demands_data.append(demand)
        
        # Convert the list of demands to a new DataFrame
        df_guaranteed_savings_charges_services = json_normalize(all_demands_data)    
                
        return df_MonthView, df_guaranteed_savings_charges, df_guaranteed_savings_charges_services

    # recebe paramatero run_getapi_GuaranteedSavingsContract
    def run_getapi_ContractManagementPeriod(self, df_MonthView):

        exec = GetDataApi()
        
        df = df_MonthView.copy()
        
        url = f"https://api.novo.thunders.com.br/gw/guaranteedsavings/ContractManagementPeriod?ContractId="
        
        list = df['id'].unique().tolist()

        # enviar lista de ids para api
        df_contract_management = exec.get_data_api_param(list, url)
        
        return df_contract_management

    # recebe paramatero run_getapi_GuaranteedSavingsContract
    def run_df_guaranteed_savings_charges_services_physicalAssets(self, df_guaranteed_savings_charges_services): 
        
        # Create an empty list to hold all the expanded demand data
        all_demands_data = []
            
        df = df_guaranteed_savings_charges_services.copy()
        
        # Iterate through each row in the DataFrame
        for index, row in df.iterrows():
            if row['physicalAssets']:  # Check if 'demands' column contains data
                demands_list = row['physicalAssets']
                
                # If it's already a list, process it
                for demand in demands_list:
                    # Add the original id and name to each demand row
                    # print(row['guaranteedSavingsContractId'],row['parent_code'])
                    
                    demand['parent_guaranteedSavingsContractId'] = row['guaranteedSavingsContractId']
                    demand['parent_start'] = row['start']
                    demand['parent_end'] = row['end']
                    demand['parent_sequence'] = row['sequence']
                    demand['parent_code'] = row['parent_code']
                    
                    # Append the demand to the list of all demand data
                    all_demands_data.append(demand)
        
        # Convert the list of demands to a new DataFrame
        df_guaranteed_savings_charges_services_physicalAssets = json_normalize(all_demands_data)
        
        # Display the resulting DataFrame
        return df_guaranteed_savings_charges_services_physicalAssets

    # recebe paramatero run_getapi_GuaranteedSavingsContract
    def run_df_guaranteed_savings_charges_services_chargeConfigurationcharges(self, df_guaranteed_savings_charges_services): 
        
        # Create an empty list to hold all the expanded demand data
        all_demands_data = []
        
        df = df_guaranteed_savings_charges_services.copy()

        # Iterate through each row in the DataFrame
        for index, row in df.iterrows():
            if row['chargeConfiguration.charges']:  # Check if 'demands' column contains data
                demands_list = row['chargeConfiguration.charges']
                #print(demands_list)
                
                # Check if 'demands' is a list (and not NaN)
                if isinstance(demands_list, list):
                    for demand in demands_list:
                        #print(demands)
                        # Add the original id and name to each demand row
                        # print(row['guaranteedSavingsContractId'],row['parent_code'])
                        demand['parent_guaranteedSavingsContractId'] = row['guaranteedSavingsContractId']
                        demand['parent_start'] = row['start']
                        demand['parent_end'] = row['end']
                        demand['parent_sequence'] = row['sequence']
                        demand['parent_code'] = row['parent_code']
                        
                        # Append the demand to the list of all demand data
                        all_demands_data.append(demand)
        
                else:
                    print(f"No valid demands for ID: {row['guaranteedSavingsContractId']}")
            
            # Convert the list of demands to a new DataFrame
            if all_demands_data:
                df_charges_services_chargeConfigurationcharges = json_normalize(all_demands_data)
            else:
                df_charges_services_chargeConfigurationcharges = pd.DataFrame()  # Empty DataFrame if no demands data
                
        # Display the resulting DataFrame
        return df_charges_services_chargeConfigurationcharges

    def run_getapi_CalculationFormula(self):

        exec = GetDataApi()
        
        url = "https://api.novo.thunders.com.br/gw/guaranteedsavings/CalculationFormula"
        
        data_json = exec.get_data_api( url)
        
        df_CalculationFormula = pd.DataFrame()
        
        df_CalculationFormula = pd.json_normalize(data_json)
        
        #df_final = pd.concat([df_final, df], axis=0)
        
        df_CalculationFormula.reset_index(drop=True, inplace=True)
            
        return df_CalculationFormula  

    def run_getapi_AdjustmentPatterns(self):

        exec = GetDataApi()
        
        url = "https://api.novo.thunders.com.br/gw/guaranteedsavings/AdjustmentPatterns"
        
        data_json = exec.get_data_api( url)
        
        df_AdjustmentPatterns = pd.DataFrame()
        
        df_AdjustmentPatterns = pd.json_normalize(data_json)
        
        #df_final = pd.concat([df_final, df], axis=0)
        
        df_AdjustmentPatterns.reset_index(drop=True, inplace=True)
            
        return df_AdjustmentPatterns  
