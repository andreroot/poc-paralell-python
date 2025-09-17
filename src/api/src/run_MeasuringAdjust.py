import concurrent.futures

from pipeline.measuring.pipeline_data_raw_measuring import GetDataMeasuring
from prepared_table.measuring.prepared_schema_table_measuring_paralell import PreparedSchemaTableMeasuringParalell


from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

import time
import sys
import os

#iniciar implementacao do envio kinesis - 06/10
#
#importar biblioteca importada pelo github: github.com/grupo-safira/geralog-monitoria.git
#
from geralog.decorator_log.monitoria import *


def get_measuringadjust(df_MeasuringPoints):
    
    tab="MeasuringAdjust"
    
    # MONITORIA DOS PROCESSOS GERAR DADOS DA API | INSERÇÃO DE DADOS
    @stream_log_method_decorator('dados/api-thunders/pipeline_MeasuringAdjust')
    def f_MeasuringAdjust():
        msg = f" | {tab} |  2/3 | Processamento paralelo para preparar o retorno json das apis"
        print(datetime.datetime.now(),f"{msg.upper()}\n")
        
        getapi = GetDataMeasuring()
        df_MeasuringAdjust = getapi.get_api_MeasuringAdjust_paralell( df_MeasuringPoints)
            
        msg = f" | {tab} | 3/3 | Preparar inserção dos dados novos no SQL Server"
        print(datetime.datetime.now(),f"{msg.upper()}\n") 
        
        exec = PreparedSchemaTableMeasuringParalell()
        exec.execute_thread_pool_paralell(df_MeasuringAdjust, 'Treinamento', 'lkok', 'MeasuringAdjust')
                        
        msg = f"tabela {tab} gerada | total de linhas:{len(df_MeasuringAdjust)}"
        return msg
        
    f_MeasuringAdjust()

def get_measuringadjustconsolidate(df_MeasuringPoints):
    
    tab="MeasuringAdjustConsolidated"

    # MONITORIA DOS PROCESSOS GERAR DADOS DA API | INSERÇÃO DE DADOS
    @stream_log_method_decorator('dados/api-thunders/pipeline_MeasuringAdjustConsolidated')
    def f_MeasuringAdjustConsolidated():

        msg = f" | {tab} | 2/3 | Processamento paralelo para preparar o retorno json das apis"
        print(datetime.datetime.now(),f"{msg.upper()}\n")
        getapi = GetDataMeasuring()
        df_MeasuringAdjustConsolidated = getapi.get_api_MeasuringAdjustConsolidated_paralell( df_MeasuringPoints )
            
        msg = f" | {tab} | 3/3 | Preparar inserção dos dados novos no SQL Server"
        print(datetime.datetime.now(),f"{msg.upper()}\n") 

        exec = PreparedSchemaTableMeasuringParalell()        
        exec.execute_thread_pool_paralell(df_MeasuringAdjustConsolidated, 'Treinamento', 'lkok', 'MeasuringAdjustConsolidated') 
                        
        msg = f"tabela {tab} gerada | total de linhas:{len(df_MeasuringAdjustConsolidated)}"
        return msg
        
    f_MeasuringAdjustConsolidated()

def get_measuringprojectionconsolidatemonthyear(df_MeasuringPoints):
    
    tab="MeasuringProjectionConsolidateMonthYear"

    # MONITORIA DOS PROCESSOS GERAR DADOS DA API | INSERÇÃO DE DADOS
    @stream_log_method_decorator('dados/api-thunders/pipeline_MeasuringProjectionConsolidateMonthYear')
    def f_MeasuringProjectionConsolidateMonthYear():

        msg = f" | {tab} | 2/3 | Processamento paralelo para preparar o retorno json das apis"
        print(datetime.datetime.now(),f"{msg.upper()}\n")
        getapi = GetDataMeasuring()
        df_MeasuringProjectionConsolidateMonthYear = getapi.get_api_MeasuringProjectionConsolidateMonthYear_paralell( df_MeasuringPoints)
        
            
        msg = f" | {tab} | 3/3 | Preparar inserção dos dados novos no SQL Server"
        print(datetime.datetime.now(),f"{msg.upper()}\n") 
        
        exec = PreparedSchemaTableMeasuringParalell()
        exec.execute_thread_pool_paralell(df_MeasuringProjectionConsolidateMonthYear, 'Treinamento', 'lkok', 'MeasuringProjectionConsolidateMonthYear') 
                        
        msg = f"tabela {tab} gerada | total de linhas:{len(df_MeasuringProjectionConsolidateMonthYear)}"
        
        return msg
    f_MeasuringProjectionConsolidateMonthYear()

    
if __name__=='__main__':    
            
    msg = f"  Measuring | 1/3 | Preparar dados para processamento, listagem de ids"
    print(datetime.datetime.now(),f"{msg.upper()}\n")
    # df_MeasuringPoints, df_MeasuringPoint_details, df_api_MeasuringPoint_associatedAssets = run_getapi_MeasuringPoint()
    
    getapi = GetDataMeasuring()   
    df_MeasuringPoints, df_MeasuringPoint_details, df_api_MeasuringPoint_associatedAssets = getapi.run_getapi_MeasuringPoint()
    
    # func=sys.argv[1]
    # print(func)

    get_measuringadjustconsolidate(df_MeasuringPoints)

    # tasks = [get_measuringadjust(df_MeasuringPoints), get_measuringprojectionconsolidatemonthyear(df_MeasuringPoints), get_measuringadjustconsolidate(df_MeasuringPoints)]
                    
    # with ProcessPoolExecutor(max_workers=3)  as executor:
        
    #     # gerar bases finance
    #     # l_tasks = [get_thunders_payments_expense, get_thunders_payments_incomes, get_thunders_payments_payments]
    #     results = executor.map(tasks)


    # # callback function to call when a task is completed
    # def custom_callback(future):
    #     print('The custom callback was called.')
        
    # with ProcessPoolExecutor(max_workers=10)  as executor:
        
    #     # gerar bases em paralelo usando como parametro uma unica base gerada df_MeasuringPoints
    #     l_task=[get_measuringadjust, get_measuringprojectionconsolidatemonthyear, get_measuringadjustconsolidate]
        
    #     # execute the task
    #     list_futures = [executor.submit(taks, df_MeasuringPoints) for taks in l_task]
        
    #     concurrent.futures.wait(list_futures)
        
    #     for future in as_completed(list_futures):
    #         # add the custom callback
    #         future.add_done_callback(custom_callback)
    #         results = future.result() 
    #         print("Waiting 60 seconds before the next batch...")
    #         time.sleep(60)                
    #         # print(results)
        
    #     # futurex = executor.submit(get_measuringadjustconsolidate , df_MeasuringPoints)
    #     # futurex.result() 

    
