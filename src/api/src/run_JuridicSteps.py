import concurrent.futures

from pipeline.juridic.pipeline_data_raw_juridic import GetDataJuridic
from prepared_table.juridic.prepared_schema_table_juridic import PreparedSchemaTableJuridic
from conectores.conectar_sql_server import ConectSqlServer

from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

from geralog.decorator_log.monitoria import *


def get_JuridicSteps(Thunders):
    tab = 'JuridicSteps'
    # MONITORIA DOS PROCESSOS GERAR DADOS DA API | INSERÇÃO DE DADOS
    @stream_log_method_decorator(f'dados/api-thunders-{Thunders}/pipeline_JuridicSteps')
    def f_iuridicstep():

        msg = f" | {tab} | 2/3 | Processamento paralelo para preparar o retorno json das apis"
        print(datetime.datetime.now(),f"{msg.upper()}\n")        

        getapi = GetDataJuridic(conn=ConectSqlServer())  
        df_juridic = getapi.run_getapi_juridic_workflowitem(Thunders, extract_full=False)

            
        msg = f" | {tab} | 3/3 | Preparar inserção dos dados novos no SQL Server"
        print(datetime.datetime.now(),f"{msg.upper()}\n") 
        
        exec = PreparedSchemaTableJuridic()
        exec.execute_thread_pool_paralell( df_juridic, Thunders, 'apiworkflows', 'JuridicSteps', extract_full=False)
                        
        msg = f"tabela {tab} gerada | total de linhas:{len(df_juridic)}"
        
        return msg
    f_iuridicstep()

    
if __name__=='__main__':    
            
    msg = f"  Juridic | 1/3 | Preparar dados para processamento, listagem de ids"
    print(datetime.datetime.now(),f"{msg.upper()}\n")
    # df_MeasuringPoints, df_MeasuringPoint_details, df_api_MeasuringPoint_associatedAssets = run_getapi_MeasuringPoint()
    
    # func=sys.argv[1]
    # print(func)

    # tasks = [get_JuridicSteps('Book'),get_JuridicSteps('BookComercial')]
                    
    # with ProcessPoolExecutor(max_workers=4)  as executor:
        
    #     # gerar bases finance
    #     # l_tasks = [get_thunders_payments_expense, get_thunders_payments_incomes, get_thunders_payments_payments]
    #     results = executor.map(tasks)

    tasks = ['Book','BookComercial']
    
    with ThreadPoolExecutor(max_workers=10)  as executor:
        
        # gerar 3 bases em paralelo
        futures = [executor.submit(get_JuridicSteps, database) for database in tasks]

        for future in as_completed(futures):
            results = future.result() 
            
            # if results is not None:
            #     result_concat = pd.concat([result_concat, results])
        
        executor.shutdown(wait=True)