from pipeline.finance.pipeline_data_raw_finance import GetDataFinance
from prepared_table.finance.prepared_schema_table_finance import PreparedSchemaTableFinance

from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import time

#iniciar implementacao do envio kinesis - 06/10
#
#importar biblioteca importada pelo github: github.com/grupo-safira/geralog-monitoria.git
#
from geralog.decorator_log.monitoria import *

def get_paymentconditions_data():
    
    tab="paymentconditions_data"
    
    # MONITORIA DOS PROCESSOS GERAR DADOS DA API | INSERÇÃO DE DADOS
    @stream_log_method_decorator('dados/api-thunders/pipeline_paymentconditions_data')
    def f_paymentconditions_data():
        msg = f" | {tab} |  2/3 | Processamento paralelo para inserção no SQL Server"
        print(datetime.datetime.now(),f"{msg.upper()}\n")
        
        getapi = GetDataFinance()
        df_paymentconditions_data = getapi.run_getapi_paymentconditions()
            
        msg = f" | {tab} | 3/3 | Preparar inserção dos dados novos no SQL Server"
        print(datetime.datetime.now(),f"{msg.upper()}\n") 
        
        exec = PreparedSchemaTableFinance()
        exec.execute_prepared_schema_table(df_paymentconditions_data,'paymentconditions_data') 
                        
        msg = f"tabela {tab} gerada | total de linhas:{len(df_paymentconditions_data)}"
        return msg
        
    f_paymentconditions_data()    
   
    

def get_InvoiceExpirationType():
    
    tab="InvoiceExpirationType"
    
    # MONITORIA DOS PROCESSOS GERAR DADOS DA API | INSERÇÃO DE DADOS
    @stream_log_method_decorator('dados/api-thunders/pipeline_InvoiceExpirationType')
    def f_InvoiceExpirationType():
        msg = f" | {tab} |  2/3 | Processamento paralelo para inserção no SQL Server"
        print(datetime.datetime.now(),f"{msg.upper()}\n")
        
        getapi = GetDataFinance()
        df_InvoiceExpirationType = getapi.run_getapi_InvoiceExpirationType()
            
        msg = f" | {tab} | 3/3 | Preparar inserção dos dados novos no SQL Server"
        print(datetime.datetime.now(),f"{msg.upper()}\n") 
        
        exec = PreparedSchemaTableFinance()
        exec.execute_prepared_schema_table(df_InvoiceExpirationType,'InvoiceExpirationType') 
                        
        msg = f"tabela {tab} gerada | total de linhas:{len(df_InvoiceExpirationType)}"
        return msg
        
    f_InvoiceExpirationType()  
     
          
    
if __name__=='__main__':    

    msg = f" Finance | 1/3 | Preparar dados para processamento, listagem de ids"
    print(datetime.datetime.now(),f"{msg.upper()}\n")
    
    tasks = [get_paymentconditions_data(), get_InvoiceExpirationType()]
                   
    with ProcessPoolExecutor(max_workers=3)  as executor:
        results = executor.map(tasks)
        

    
