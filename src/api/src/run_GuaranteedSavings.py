from pipeline.guaranted.pipeline_data_raw_guaranted import GetDataGuaranted
from prepared_table.guaranted.prepared_schema_table_guaranted import PreparedSchemaTableGuaranted

from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import time

#iniciar implementacao do envio kinesis - 06/10
#
#importar biblioteca importada pelo github: github.com/grupo-safira/geralog-monitoria.git
#
from geralog.decorator_log.monitoria import *

def get_guaranteed_savings_charges(df):
    
    tab="guaranteed_savings_charges"
    
    # MONITORIA DOS PROCESSOS GERAR DADOS DA API | INSERÇÃO DE DADOS
    @stream_log_method_decorator('dados/api-thunders/pipeline_guaranteed_savings_charges')
    def f_guaranteed_savings_charges():

        msg = f" | {tab} | 3/3 | Preparar inserção dos dados novos no SQL Server"
        print(datetime.datetime.now(),f"{msg.upper()}\n") 
        
        exec = PreparedSchemaTableGuaranted()
        exec.execute_prepared_schema_table(df,'guaranteed_savings_charges') 
                        
        msg = f"tabela {tab} gerada | total de linhas:{len(df)}"
        return msg
        
    f_guaranteed_savings_charges()
    
def get_guaranteed_savings_charges_services(df):
    
    tab="guaranteed_savings_charges_services"
    
    # MONITORIA DOS PROCESSOS GERAR DADOS DA API | INSERÇÃO DE DADOS
    @stream_log_method_decorator('dados/api-thunders/pipeline_guaranteed_savings_charges_services')
    def f_guaranteed_savings_charges_services():

        msg = f" | {tab} | 3/3 | Preparar inserção dos dados novos no SQL Server"
        print(datetime.datetime.now(),f"{msg.upper()}\n") 
        
        exec = PreparedSchemaTableGuaranted()
        exec.execute_prepared_schema_table(df,'guaranteed_savings_charges_services') 
                        
        msg = f"tabela {tab} gerada | total de linhas:{len(df)}"
        return msg
        
    f_guaranteed_savings_charges_services()

def get_month_view(df):
    
    tab="MonthView"
    
    # MONITORIA DOS PROCESSOS GERAR DADOS DA API | INSERÇÃO DE DADOS
    @stream_log_method_decorator('dados/api-thunders/pipeline_MonthView')
    def f_month_view():

        msg = f" | {tab} | 3/3 | Preparar inserção dos dados novos no SQL Server"
        print(datetime.datetime.now(),f"{msg.upper()}\n") 
        
        exec = PreparedSchemaTableGuaranted()
        exec.execute_prepared_schema_table(df,'MonthView') 
                        
        msg = f"tabela {tab} gerada | total de linhas:{len(df)}"
        return msg
        
    f_month_view()
    

def get_ContractManagementPeriod(df):
    
    tab="ContractManagementPeriod"
    
    # MONITORIA DOS PROCESSOS GERAR DADOS DA API | INSERÇÃO DE DADOS
    @stream_log_method_decorator('dados/api-thunders/pipeline_ContractManagementPeriod')
    def f_ContractManagementPeriod():

        msg = f" | {tab} | 3/3 | Preparar inserção dos dados novos no SQL Server"
        print(datetime.datetime.now(),f"{msg.upper()}\n") 
        
        exec = PreparedSchemaTableGuaranted()
        exec.execute_prepared_schema_table(df,'ContractManagementPeriod') 
                        
        msg = f"tabela {tab} gerada | total de linhas:{len(df)}"
        return msg
        
    f_ContractManagementPeriod()    

def get_guaranteed_savings_charges_services_physicalAssets(df):
    
    tab="guaranteed_savings_charges_services_physicalAssets"
    
    # MONITORIA DOS PROCESSOS GERAR DADOS DA API | INSERÇÃO DE DADOS
    @stream_log_method_decorator('dados/api-thunders/pipeline_guaranteed_savings_charges_services_physicalAssets')
    def f_guaranteed_savings_charges_services_physicalAssets(df):
        msg = f" | {tab} |  2/3 | Processamento paralelo para inserção no SQL Server"
        print(datetime.datetime.now(),f"{msg.upper()}\n")
        
        getapi = GetDataGuaranted()
        df_guaranteed_savings_charges_services_physicalAssets = getapi.run_df_guaranteed_savings_charges_services_physicalAssets(df)
            
        msg = f" | {tab} | 3/3 | Preparar inserção dos dados novos no SQL Server"
        print(datetime.datetime.now(),f"{msg.upper()}\n") 
        
        exec = PreparedSchemaTableGuaranted()
        exec.execute_prepared_schema_table(df_guaranteed_savings_charges_services_physicalAssets,'guaranteed_savings_charges_services_physicalAssets') 
                        
        msg = f"tabela {tab} gerada | total de linhas:{len(df)}"
        return msg
        
    f_guaranteed_savings_charges_services_physicalAssets(df)
    


def get_guaranteed_savings_charges_services_chargeConfigurationcharges(df):
    
    tab="guaranteed_savings_charges_services_chargeConfigurationcharges"
    
    # MONITORIA DOS PROCESSOS GERAR DADOS DA API | INSERÇÃO DE DADOS
    @stream_log_method_decorator('dados/api-thunders/pipeline_guaranteed_savings_charges_services_chargeConfigurationcharges')
    def f_guaranteed_savings_charges_services_chargeConfigurationcharges(df):
        msg = f" | {tab} |  2/3 | Processamento paralelo para inserção no SQL Server"
        print(datetime.datetime.now(),f"{msg.upper()}\n")
        
        getapi = GetDataGuaranted()
        df_charges_services_chargeConfigurationcharges = getapi.run_df_guaranteed_savings_charges_services_chargeConfigurationcharges(df)
            
        msg = f" | {tab} | 3/3 | Preparar inserção dos dados novos no SQL Server"
        print(datetime.datetime.now(),f"{msg.upper()}\n") 
        
        exec = PreparedSchemaTableGuaranted()
        exec.execute_prepared_schema_table(df_charges_services_chargeConfigurationcharges,'guaranteed_savings_charges_services_chargeConfigurationcharges') 
                        
        msg = f"tabela {tab} gerada | total de linhas:{len(df)}"
        return msg
        
    f_guaranteed_savings_charges_services_chargeConfigurationcharges(df)    
    
    
if __name__=='__main__':    

    msg = f" GuaranteedSavings | 1/3 | Preparar dados para processamento, listagem de ids"
    print(datetime.datetime.now(),f"{msg.upper()}\n")
    # df_MeasuringPoints, df_MeasuringPoint_details, df_api_MeasuringPoint_associatedAssets = run_getapi_MeasuringPoint()
    getapi = GetDataGuaranted()
    df_MonthView, df_guaranteed_savings_charges, df_guaranteed_savings_charges_services = getapi.run_getapi_GuaranteedSavingsContract()

    tasks = [get_month_view(df_MonthView)
            , get_ContractManagementPeriod(df_MonthView)
            , get_guaranteed_savings_charges(df_guaranteed_savings_charges)
            , get_guaranteed_savings_charges_services_physicalAssets(df_guaranteed_savings_charges_services)
            , get_guaranteed_savings_charges_services(df_guaranteed_savings_charges_services)
            , get_guaranteed_savings_charges_services_chargeConfigurationcharges(df_guaranteed_savings_charges_services)
            ]
    
    with ProcessPoolExecutor(max_workers=6)  as executor:
        results = executor.map(tasks)
        
    
