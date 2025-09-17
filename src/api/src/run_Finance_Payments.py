from pipeline.finance.pipeline_data_raw_finance import GetDataFinance
from prepared_table.finance.prepared_schema_table_finance_thunders_paralell import PreparedSchemaTableFinanceThundersParalell
from prepared_table.finance.validation_columns import get_columns_sql


from conectores.conectar_sql_server import ConectSqlServer

from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import time

from geralog.decorator_log.monitoria import *
from resources.send_webhook import webhook

def execute_error(e):
    return e

def get_thunders_payments_payments(database, cenario):

    def execute():
        try:
            msg = f" | Payments |  2/3 | Processamento paralelo preparando dados das APIs"
            print(datetime.datetime.now(),f"{msg.upper()}\n")
            
            getapi = GetDataFinance()

            df_income_to_sql = getapi.run_getapi_payments_incomes(database,cenario)
            df_expenses_to_sql = getapi.run_getapi_payments_expenses(database,cenario)
            
            df_payments = pd.concat([df_income_to_sql, df_expenses_to_sql], ignore_index=True)
            print(datetime.datetime.now(), len(df_payments))
            
            msg = f" | Payments |  3/3 | Processamento paralelo para inserção no SQL Server"
            print(datetime.datetime.now(),f"{msg.upper()}\n")
            
            exec = PreparedSchemaTableFinanceThundersParalell()
            exec.execute_thread_pool_paralell( df_payments, database, 'APIpayments', 'Payments')

            # VALIDACAO DO PROCESSO DE INSERCAO
            msg = f" | Payments | 4/4 | Validando inserção no SQL Server"
            print(datetime.datetime.now(),f"{msg.upper()}\n")
            
            conn = ConectSqlServer()
            get_engine_sqlalchemy = conn.get_engine_sqlalchemy(database)

            sql_query = 'SELECT * FROM {database}.APIpayments.Payments'.format(database=database)

            df = pd.DataFrame()                
            df = pd.read_sql_query(sql_query, get_engine_sqlalchemy)
                                              
            if len(df)==0:
                msg = f"tabela {database}.APIpayments.Payments não possui dados"
                print(msg)
                
                # validacao das colunas da API e das existentes nas tabelas do SQL Server
                dfcol= get_columns_sql(database) # colunas da tabela SQL Server
                
                # print(f"columns: {dfcol}")
                print(f"total de columns - SQLServer: {len(dfcol)+1}")  # +1 para considerar a coluna 'DataInsertTable'
                # print(f"columns: {df_payments.columns.tolist()}")  
                print(f"total de columns - API: {len(df_payments.dtypes.tolist())}")   
                
                if len(dfcol)+1 > len(df_payments.columns.tolist()): 
                    webhook(f'dados/api-thunders_{database}/pipeline_payments_Payments', "Dados não inseridos, pois as colunas não estão de acordo com o esperado")

                    decorator_monitoria = stream_log_method_decorator_error(f'dados/api-thunders/pipeline_payments_Payments')(execute_error)
                    decorator_monitoria("Dados não inseridos, pois as colunas não estão de acordo com o esperado")
                    
    
                elif len(dfcol)+1 == len(df_payments.columns.tolist()):  
                                    
                    webhook(f'dados/api-thunders_{database}/pipeline_payments_Payments', "Não ha dados")

                    decorator_monitoria = stream_log_method_decorator_error(f'dados/api-thunders/pipeline_payments_Payments')(execute_error)
                    decorator_monitoria("Não ha dados")
                    
            msg = f"tabela {database}.APIpayments.Payments gerada | total de linhas geradas na api:{len(df_payments)} | total de linhas inseridas:{len(df)} "
            return msg
        
        except Exception as e:
            webhook(f'dados/api-thunders_{database}/pipeline_payments_Payments', e)
            
            decorator_monitoria = stream_log_method_decorator_error(f'dados/api-thunders/pipeline_payments_Payments')(execute_error)
            decorator_monitoria(f'ERROR:{e}')            
            
            return f'ERROR:{e}'            
        # except exc.SQLAlchemyError as e:
        #     # webhook(tag_monitoria, e)
        #     decorator_monitoria = stream_log_method_decorator_error(f'dados/api-thunders_{database}/pipeline_payments_Payments')(execute_error)
        #     decorator_monitoria(f'ERROR:{e}')            
        #     return f'ERROR:{e}'                        
        # except pyodbc.Error as e:
        #     # webhook(tag_monitoria, e)
        

                                
    decorator_monitoria = stream_log_method_decorator(f'dados/api-thunders_{database}/pipeline_payments_Payments')(execute)
    decorator_monitoria()          
    
if __name__=='__main__':    

    msg = f" Finance | 1/3 | Preparar dados para processamento, listagem de ids"
    print(datetime.datetime.now(),f"{msg.upper()}\n")
    
    tasks = [( 'Book', '122'), ( 'BookComercial', '13')]
                    
    with ProcessPoolExecutor(max_workers=2)  as executor:
        #results = executor.map(tasks)
        futures = [executor.submit(get_thunders_payments_payments, database, cenario) for database, cenario in tasks]
        
        for future in as_completed(futures):
            results = future.result() 
            print("Waiting 60 seconds before the next batch...")
            time.sleep(60)
                                
        executor.shutdown(wait=True)

    
