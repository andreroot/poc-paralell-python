from pipeline.measuring.pipeline_data_raw_measuring import GetDataMeasuring 
from resources.prepared_schema_table import PreparedSchemaTable

from concurrent.futures import ThreadPoolExecutor, as_completed


#iniciar implementacao do envio kinesis - 06/10
#
#importar biblioteca importada pelo github: github.com/grupo-safira/geralog-monitoria.git
#
from geralog.decorator_log.monitoria import *

   
def get_MeasuringPoint( df, tabx ):
    # MONITORIA DOS PROCESSOS NA INSERÇÃO DE DADOS
    @stream_log_method_decorator(f'dados/api-thunders/pipeline_{tabx}')
    def f_MeasuringPoint():
        msg = f"{tabx} | 2/3 | Processamento paralelo para inserção no SQL Server"
        print(datetime.datetime.now(),f"{msg.upper()}\n") 
        
        msg = f"{tabx} | 3/3 | Preparar inserção dos dados novos no SQL Server"            
        print(datetime.datetime.now(),f"{msg.upper()}\n")
        
        exec = PreparedSchemaTable()
        exec.execute_prepared_schema_table(df, tabx)
        msg = f"tabela {tabx} gerada | total de linhas:{len(df)}"
        #| tabela MeasuringPoint_details gerada | total de linhas:{len(df_MeasuringPoint_details)} | tabela MeasuringPoint_associatedAssets gerada | total de linhas:{len(df_api_MeasuringPoint_associatedAssets)}"

        return msg
    f_MeasuringPoint()


        
if __name__=='__main__':    
    
    tab="MeasuringPoint"
    
    msg = f"{tab} | 1/3 | Prepara dados para processamento, listagem de ids"
    
    print(datetime.datetime.now(),f"{msg.upper()}\n")

    run = GetDataMeasuring()
    # Executa a função para obter os dados da API e retorna os DataFrames necessários
    # df_MeasuringPoints, df_MeasuringPoint_details, df_api_MeasuringPoint_associatedAssets = run.run_getapi_MeasuringPoint()             
    df_MeasuringPoints, df_MeasuringPoint_details, df_api_MeasuringPoint_associatedAssets = run.run_getapi_MeasuringPoint()
    
    task1=[(df_MeasuringPoints, 'MeasuringPoint'),(df_MeasuringPoint_details, 'MeasuringPoint_details'),(df_api_MeasuringPoint_associatedAssets, 'MeasuringPoint_associatedAssets')]
    with ThreadPoolExecutor(max_workers=10)  as executor:
        
        # gerar 3 bases em paralelo
        futures = [executor.submit(get_MeasuringPoint, df, tabx) for df, tabx in task1]

        for future in as_completed(futures):
            results = future.result() 
            
            # if results is not None:
            #     result_concat = pd.concat([result_concat, results])
        
        executor.shutdown(wait=True)