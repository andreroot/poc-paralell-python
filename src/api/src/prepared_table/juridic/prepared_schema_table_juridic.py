from prepared_table.insert_sql_server import InsertSqlServer
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import datetime
import time
from sqlalchemy import types

class PreparedSchemaTableJuridic:
    
    def __init__(self):
        self.database = 'TREINAMENTO'
        self.schema = 'apiworkflows'
            
    def execute_prepared_schema_table(self, df, Thunders, schema, tab):

        ins = InsertSqlServer()

        forced_dtype = {

            'stepId': types.String(length=255),
            'stepName': types.String(length=255),
            'Contratos': types.String(length=255),
            'daysInStep': types.String(length=255),
            'stepOrder': types.String(length=255),
            'stepType': types.String(length=255),
            'lastTimestamp': types.DateTime,
            'currentWorkflowStepId': types.String(length=255),
            'currentWorkflowId': types.String(length=255),
            'currentWorkflowItemId': types.String(length=255),
            'currentContractCode': types.String(length=255),
            'currentWorkflowName': types.String(length=255),
            'currentWorkflowIsActive': types.Boolean,
            'currentStepBlockEditing': types.Boolean,
            'currentWorkflowCategoryId': types.String(length=255)
                    }

        columns_with_lists = ['lastTimestamp']
        df = ins.convert_column_to_datetime(df, columns_with_lists)      
          
        # Assuming df_final is your DataFrame
        # print(df.columns)
        # print(df.dtypes)
        # print(df.head(1))
        
        ins.insert_dataframe_from_table(df, Thunders, schema, tab, 'append',  forced_dtype)
        #ins.insert_sql_from_table( df, Thunders, schema, tab)


    # PROCESSO PARALELO COM CHUCKSIZE DO DATFRAME E EXECUÇÃO DE CADA PARTE
    def execute_thread_pool_paralell(self, df, Thunders, schema, tab, extract_full=True):

        # Define o tamanho do chunk (exemplo: 5000 registros por inserção)
        chunksize = 5000

        # Divide o DataFrame em chunks
        chunks = [df[i:i+chunksize] for i in range(0, len(df), chunksize)]
        
        # função para execução de isntrução sql
        ins = InsertSqlServer()

        
        if extract_full:
            # ETAPA DELEÇÃO: DELETE FROM

            msg = f" | {tab} | FULL | DELEÇÃO dados existentes na tabela no SQL Server"
            print(datetime.datetime.now(),f"{msg.upper()}\n")   
                      
            # Deletar todos os dados da tabela
            ins.delete_from_table_all(df, Thunders, schema, tab)
        else:  
            # Deletar apenas os dados que estão no dataframe
            # ins.delete_from_table(df, Thunders, schema, tab)
            # Deletar todos os dados da tabela
            msg = f" | {tab} | INCREMENTAL | DELEÇÃO dados existentes na tabela no SQL Server"
            print(datetime.datetime.now(),f"{msg.upper()}\n")   

            ins.delete_from_table_juridic_incremental(df, Thunders, schema, tab)
            
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures=[executor.submit(self.execute_prepared_schema_table, chunckies, Thunders, schema, tab) for   chunckies in chunks]
                
            for future in as_completed(futures):
                future.result()
                #time.sleep(60)
        
            executor.shutdown(wait=True)