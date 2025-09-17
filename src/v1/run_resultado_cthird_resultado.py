
import datetime
import pandas as pd
import platform
import json

from sqlalchemy import  exc, text

from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

from conectores.conectar_sql_server2 import ConectSqlServer
from resourcex.utilitarios import Utils

# import dynamically.dynamically as main_dynamically
from geralog.decorator_log.monitoria import *


# def webhook(tag_monitoria, erro):
#     with open(f"webhook.json", "r", encoding="utf-8") as config_file:
#         config = json.load(config_file)    
    
#     print(config['webhook'])
    
#     config["webhook"]["parametros_webhook"]["tag_monitoria"] = tag_monitoria
#     config["webhook"]["parametros_webhook"]["erro"] = erro
#     webhook_url = config["webhook"]["msg_webhook"]["url_webhook"]

#     info = config["webhook"]["msg_webhook"]["info1"]
#     dash = config["webhook"]["msg_webhook"]["dash"]
#     # print(info)
#     main_dynamically.send_webhook_message(webhook_url, dash, info.format(**config["webhook"]["parametros_webhook"])) 

def execute_error(e):
    return e

def extract_operations(database, tag_monitoria, file, file2):
    #exec_historico_posicaocontraparte_log
    # @stream_log_method_decorator(f'sql-server/{tag_monitoria}') 
    def execute():
        
        conn = ConectSqlServer()
        
        utils = Utils()
        
        msg = f" BASES {tag_monitoria} | 2/2 | EXECUTANDO QUERY NO SQL SERVER/n"
        print(datetime.datetime.now(),f"{msg.upper()}\n")
                                    
        so = platform.system()
        
        # executa PROC file
        get_engine_sqlalchemy = conn.get_engine_sqlalchemy(database)
        
        # script_proc = utils.read_file(file)  

        # conn = get_engine_sqlalchemy.connect()
        
        # conn.execute(text(script_proc))

        # Feche a conexão
        conn.close()        
        
        # cursor = engine.cursor()
        # cursor.fast_executemany = True  # Habilita otimização

        # cursor.execute(script_proc)
        # engine.commit()
        
        # cursor.close()
        # engine.close()    
                        
                                                
        # EXECUTA SELECT COUNT file2
        # get_engine_sqlalchemy = conn.get_engine_sqlalchemy(database)
    
        sql_query = utils.read_file(file2)  
            
                        
        df = pd.DataFrame()

            
        df = pd.read_sql_query(sql_query, get_engine_sqlalchemy)

        # if len(df)==0:
        #     webhook(tag_monitoria, 'NÃO HA DADOS')

        #     decorator_monitoria = stream_log_method_decorator_error(f'sql-server/{tag_monitoria}')(execute_error)
        #     decorator_monitoria("Não ha dados")
        print(f"/nExecutado com sucesso: total de linhas atualizadas {len(df)}/n")
        
        return f"Executado com sucesso: total de linhas atualizadas {len(df)}"

                                 
    decorator_monitoria = stream_log_method_decorator(f'sql-server/{tag_monitoria}')(execute)
    decorator_monitoria()
    
if __name__=='__main__':
    
    
    msg = f" BASES [book].[HistoricoResultado_log] | 1/3 | GERAR BASES HISTORICOS DE RESULTADO/n"
    print(datetime.datetime.now(),f"{msg.upper()}\n")
    
    extract_operations("Book","exec_historico_resultado_log","proc_book_HistoricoResultado_log.sql","proc_count_book_HistoricoResultado_log.sql")    