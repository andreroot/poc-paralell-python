
import datetime
import pandas as pd
import platform
import json

from sqlalchemy import  exc
import pyodbc

from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

from conectores.conectar_sql_server import ConectSqlServer
from resourcex.utilitarios import Utils

#iniciar implementacao do envio kinesis - 06/10
#
#importar biblioteca importada pelo github: github.com/grupo-safira/geralog-monitoria.git
#
from geralog.decorator_log.monitoria import *
import dynamically.dynamically as main_dynamically

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

        print(script_proc)                         
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
        
        # print(len(df))
        if len(df)<100000:
            webhook('sql-server/poc_historico', 'DADOS ABAIXO DE <100K')
            
            decorator_monitoria = stream_log_method_decorator_error(f'sql-server/poc_historico')(execute_error)
            decorator_monitoria("Dados abaixo de 100k")
            
        return df
    except Exception as e:
        webhook('sql-server/poc_historico', e)
        
    except exc.SQLAlchemyError as e:
        webhook('sql-server/poc_historico', e)
                    
    except pyodbc.Error as e:
        webhook('sql-server/poc_historico', e)
    
if __name__=='__main__':
    
    msg = f" BASES [modelo].[BaseHistorica].[BoletasProcessadasv2] | 1/3 | GERAR BASES HISTORICOS DE RESULTADO"
    print(datetime.datetime.now(),f"{msg.upper()}\n")
    
    decorator_executor = stream_log_method_decorator('sql-server/poc_historico')(extract_operations)
    decorator_executor("Book","proc_STP_gera_bases_POC_Historico_portifolio.sql","proc_count_STP_gera_bases_POC_Historico_portifolio.sql")       
    