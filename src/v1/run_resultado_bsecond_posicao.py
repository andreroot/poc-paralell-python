
import datetime
import pandas as pd
import platform
import json

from sqlalchemy import  exc
import pyodbc

from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

from conectores.conectar_sql_server import ConectSqlServer
from resourcex.utilitarios import Utils

import dynamically.dynamically as main_dynamically
from geralog.decorator_log.monitoria import *


def webhook(tag_monitoria, erro):
    with open(f"webhook.json", "r", encoding="utf-8") as config_file:
        config = json.load(config_file)    
    
    print(config['webhook'])
    
    config["webhook"]["parametros_webhook"]["tag_monitoria"] = tag_monitoria
    config["webhook"]["parametros_webhook"]["erro"] = erro
    webhook_url = config["webhook"]["msg_webhook"]["url_webhook"]

    info = config["webhook"]["msg_webhook"]["info1"]
    # print(info)
    main_dynamically.send_webhook_message(webhook_url, info.format(**config["webhook"]["parametros_webhook"])) 
 
def execute_error(e):
    return e
 
def extract_operations(database, tag_monitoria, file, file2):
    #exec_historico_posicaocontraparte_log
    # @stream_log_method_decorator(f'sql-server/{tag_monitoria}') 
    def execute():
        
        try:
            conn = ConectSqlServer()
            
            utils = Utils()
            
            msg = f" BASES {tag_monitoria} | 2/2 | EXECUTANDO QUERY NO SQL SERVER"
            print(datetime.datetime.now(),f"{msg.upper()}\n")
                                        
            so = platform.system()
            print(so)
            
            # # executa PROC file
            connectionString, engine = conn.get_engine_pyodbc(database)
            
            if so == 'Windows':
                    
                script_proc = utils.read_file_win(file)
            else:
                script_proc = utils.read_file(file)  
                                                        
            cursor = engine.cursor()
            cursor.fast_executemany = True  # Habilita otimização

            cursor.execute(script_proc)
            engine.commit()
            
            cursor.close()
            engine.close()    
                            
                                                    
            # EXECUTA SELECT COUNT file2
            get_engine_sqlalchemy = conn.get_engine_sqlalchemy(database)
        
            if so == 'Windows':
                    
                sql_query = utils.read_file_win(file2)
            else:
                sql_query = utils.read_file(file2)  
                
                            
            df = pd.DataFrame()
    
                
            df = pd.read_sql_query(sql_query, get_engine_sqlalchemy)

            if len(df)==0:
                webhook(tag_monitoria, 'NÃO HA DADOS')

                decorator_monitoria = stream_log_method_decorator_error(f'sql-server/{tag_monitoria}')(execute_error)
                decorator_monitoria("Não ha dados")
                
            return df
        except Exception as e:
            webhook(tag_monitoria, e)
            decorator_monitoria = stream_log_method_decorator_error(f'sql-server/{tag_monitoria}')(execute_error)
            decorator_monitoria(f'ERROR:{e}')            
            return f'ERROR:{e}'
                    
        except exc.SQLAlchemyError as e:
            webhook(tag_monitoria, e)
            decorator_monitoria = stream_log_method_decorator_error(f'sql-server/{tag_monitoria}')(execute_error)
            decorator_monitoria(f'ERROR:{e}')            
            return f'ERROR:{e}'                        
        except pyodbc.Error as e:
            webhook(tag_monitoria, e)
            decorator_monitoria = stream_log_method_decorator_error(f'sql-server/{tag_monitoria}')(execute_error)
            decorator_monitoria(f'ERROR:{e}')            
            return f'ERROR:{e}'            
                         
    decorator_monitoria = stream_log_method_decorator(f'sql-server/{tag_monitoria}')(execute)
    decorator_monitoria()
        
if __name__=='__main__':
    
    
    msg = f" BASES [modelo].[BaseHistorica].[BoletasProcessadasv2] | 1/3 | GERAR BASES HISTORICOS DE POSICAO"
    print(datetime.datetime.now(),f"{msg.upper()}\n")

    tasks=[("Book","exec_historico_posicao_log","proc_book_HistoricoPosicao_log.sql","proc_count_book_HistoricoPosicao_log.sql"),
           ("Book","exec_historico_posicaocontraparte_log","proc_book_HistoricoPosicaoContraParte_log.sql","proc_count_book_HistoricoPosicaoContraParte_log.sql")]       
             
    with ProcessPoolExecutor(max_workers=2)  as executor:
        # results = executor.map(tasks)

        futures = [executor.submit(extract_operations, database, tag_monitoria, file, file2) for database, tag_monitoria, file, file2 in tasks]
        # process task results as they are available
        for future in as_completed(futures):
            results = future.result() 

        executor.shutdown(wait=True)   
