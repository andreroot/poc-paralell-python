from pipeline.crm.pipeline_data_raw_crm import GetDataCRM
from prepared_table.finance.prepared_schema_table_finance import PreparedSchemaTableFinance

from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import time

#iniciar implementacao do envio kinesis - 06/10
#
#importar biblioteca importada pelo github: github.com/grupo-safira/geralog-monitoria.git
#
from geralog.decorator_log.monitoria import *

def get_loadPhysicalAssets_details():
        
    # MONITORIA DOS PROCESSOS GERAR DADOS DA API | INSERÇÃO DE DADOS
    # @stream_log_method_decorator('dados/api-thunders/pipeline_CRM_loadPhysicalAssets')
    def f_loadPhysicalAssets():
        msg = f" | tabelas CRM |  2/3 | Processamento paralelo para inserção no SQL Server"
        print(datetime.datetime.now(),f"{msg.upper()}\n")
        
        getapi = GetDataCRM()
        df_loadPhysicalAssets_details, df_demands = getapi.run_getapi_loadPhysicalAssets()
                
        exec = PreparedSchemaTableFinance()

        msg = f"tabela loadPhysicalAssets_details gerada | total de linhas:{len(df_loadPhysicalAssets_details)}"
        print(datetime.datetime.now(),f"{msg.upper()}\n")

        decorator_monitoria = stream_log_method_decorator_error(f'dados/api-thunders/pipeline_CRM_loadPhysicalAssets')(exec.execute_prepared_schema_table)
        decorator_monitoria(df_loadPhysicalAssets_details,'loadPhysicalAssets_details_v1') 

        # msg = f"tabela demands gerada | total de linhas:{len(df_demands)}"
        # print(datetime.datetime.now(),f"{msg.upper()}\n")

        # decorator_monitoria = stream_log_method_decorator_error(f'dados/api-thunders/pipeline_CRM_demands')(exec.execute_prepared_schema_table)
        # decorator_monitoria(df_demands,'demands') 
                        
        
        return msg
        
    f_loadPhysicalAssets()


    
if __name__=='__main__':    

    msg = f" Finance | 1/3 | Preparar dados para processamento, listagem de ids"
    print(datetime.datetime.now(),f"{msg.upper()}\n")
    
    tasks = [get_loadPhysicalAssets_details()]
                   
    with ProcessPoolExecutor(max_workers=3)  as executor:
        results = executor.map(tasks)
        

    
