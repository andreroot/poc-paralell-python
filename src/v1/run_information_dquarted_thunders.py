
import datetime
import pandas as pd
import platform
import json

from sqlalchemy import  exc
import pyodbc

from conectores.conectar_sql_server import ConectSqlServer
from resourcex.utilitarios import Utils

# pacotes de github - safira, customizados

# pacote dinamico enviar dados para webhook via json
import dynamically.dynamically as main_dynamically

# pacote monitoria envia dados para kinesis
from geralog.decorator_log.monitoria import *

def webhook(tag_monitoria, erro):
    with open(f"webhook.json", "r", encoding="utf-8") as config_file:
        config = json.load(config_file)    
    # config = {
    #     "webhook":{
    #                 "msg_webhook":
    #                 {
    #                     "url_webhook": "https://safira.webhook.office.com/webhookb2/dcf11fd6-f480-4438-a03e-b9088847745f@35d0502c-7339-451e-b90e-e585949701c4/IncomingWebhook/b9dd3ef27a714307b51448c9d1be8cc9/b7777832-c01a-4727-a99a-f27e0fd087fd/V2CNPcFhpM_DoMZMcn22biFddWYiBJboPU80yG504kpFM1",
    #                     "info1": F"🙀 PROCESSO:{tag_monitoria} | ERRO:{erro}, necessário verificar dash monitoria: sql-server/{tag_monitoria}"
            
    #                 }
    #             }
    #         }
    print(config['webhook'])
    config["webhook"]["parametros_webhook"]["tag_monitoria"] = tag_monitoria
    config["webhook"]["parametros_webhook"]["erro"] = erro
    webhook_url = config["webhook"]["msg_webhook"]["url_webhook"]

    info = config["webhook"]["msg_webhook"]["info1"]
    # print(info)
    main_dynamically.send_webhook_message(webhook_url, info.format(**config["webhook"]["parametros_webhook"])) 
                    

def execute_error(e):
    return e

def extract_operations(database, file, file2):
    
    try:
            
        conn = ConectSqlServer()
        
        utils = Utils()
        
        msg = f" BASES  | 2/2 | EXECUTANDO QUERY NO SQL SERVER"
        print(datetime.datetime.now(),f"{msg.upper()}\n")
        
        so = platform.system()
        print(so)
        if so == 'Windows':
                
            script_proc = utils.read_file_win(file)
        else:
            script_proc = utils.read_file(file)  

                                
        # executa PROC file
        connectionString, engine = conn.get_engine_pyodbc(database)
                
        with engine.cursor() as cursor:
            cursor.fast_executemany = True  # Habilita otimização
            cursor.execute(script_proc)
        # engine.commit()
                                    
        # with get_engine_sqlalchemy.connect() as connection:
        #     connection.execute(text(script_proc))
        #     connection.commit()  # Importante para alterações no banco!

        # EXECUTA SELECT COUNT file2
        get_engine_sqlalchemy = conn.get_engine_sqlalchemy(database)
        
        if so == 'Windows':
                
            sql_query = utils.read_file_win(file2)
        else:
            sql_query = utils.read_file(file2)  
                
        df = pd.DataFrame()
        df = pd.read_sql_query(sql_query, get_engine_sqlalchemy)
        if len(df)==0:
            webhook('sql-server/exec_informacao_comercial', 'NÃO HÁ DADOS')
            
            decorator_monitoria = stream_log_method_decorator_error(f'sql-server/exec_informacao_comercial')(execute_error)
            decorator_monitoria('NÃO HÁ DADOS')
        return df
    except Exception as e:
        webhook('sql-server/exec_informacao_comercial', e)

        decorator_monitoria = stream_log_method_decorator_error('sql-server/exec_informacao_comercial')(execute_error)
        decorator_monitoria(f'ERROR:{e}')            
        return f'ERROR:{e}'
                
    except exc.SQLAlchemyError as e:
        webhook('sql-server/exec_informacao_comercial', e)

        decorator_monitoria = stream_log_method_decorator_error('sql-server/exec_informacao_comercial')(execute_error)
        decorator_monitoria(f'ERROR:{e}')            
        return f'ERROR:{e}'
                        
    except pyodbc.Error as e:
        webhook('sql-server/exec_informacao_comercial', e)

        decorator_monitoria = stream_log_method_decorator_error('sql-server/exec_informacao_comercial')(execute_error)
        decorator_monitoria(f'ERROR:{e}')            
        return f'ERROR:{e}'            
    
if __name__=='__main__':
    
    
    msg = f" BASES [modelo].[BaseHistorica].[BoletasProcessadasv2] | 1/3 | GERAR BASES HISTORICOS DE RESULTADO"
    print(datetime.datetime.now(),f"{msg.upper()}\n")
    
    decorator_executor = stream_log_method_decorator('sql-server/exec_informacao_comercial')(extract_operations)
    
    decorator_executor("Book","proc_book_informacao_comercial.sql","proc_count_book_informacao_comercial.sql")       