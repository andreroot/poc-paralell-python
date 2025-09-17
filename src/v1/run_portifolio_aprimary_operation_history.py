import sys
from pathlib import Path
import pandas as pd
import datetime
import platform
import json

from sqlalchemy import  exc
import pyodbc

from conectores.conectar_sql_server import ConectSqlServer
from resourcex.utilitarios import Utils
import dynamically.dynamically as main_dynamically
from geralog.decorator_log.monitoria import *

sys.path.append((Path(__file__).parent.parent).resolve().as_posix())
import logging

logging.basicConfig()
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)


def webhook(tag_monitoria, erro):
    with open(f"webhook.json", "r", encoding="utf-8") as config_file:
        config = json.load(config_file)    
 
    config["webhook"]["parametros_webhook"]["tag_monitoria"] = tag_monitoria
    config["webhook"]["parametros_webhook"]["erro"] = erro
    webhook_url = config["webhook"]["msg_webhook"]["url_webhook"]

    info = config["webhook"]["msg_webhook"]["info1"]
    # print(info)
    main_dynamically.send_webhook_message(webhook_url, info.format(**config["webhook"]["parametros_webhook"])) 

def execute_error(e):
    return e

def main(file):
    try:
            
        conn = ConectSqlServer()
        utils = Utils()

        msg = f" EXECUTE dbo.STP_operation_history | Modelo.dbo.[proc_operation_history] | EXECUTANDO QUERY NO SQL SERVER"
        print(datetime.datetime.now(),f"{msg.upper()}\n")

        so = platform.system()
        print(so)
                
        # # COMENTAR PARA TESTAR ENVIO WEBHOOK
        # # # executa PROC file
        connectionString, engine = conn.get_engine_pyodbc('Modelo')
        
        script_proc = """   		
            BEGIN

                SET NOCOUNT ON;
                SET ANSI_WARNINGS OFF;
                EXEC dbo.STP_operation_history; 
            
            END;
            
            """

        with engine.cursor() as cursor:
            cursor.fast_executemany = True  # Habilita otimização
            cursor.execute(script_proc)

        # EXECUTA SELECT COUNT file2
        get_engine_sqlalchemy = conn.get_engine_sqlalchemy('Modelo')
        
        df = pd.DataFrame()
        
        if so == 'Windows':
                
            sql_query = utils.read_file_win(file)
        else:
            sql_query = utils.read_file(file)  
                
        df = pd.DataFrame()
        df = pd.read_sql_query(sql_query, get_engine_sqlalchemy)
                    
        # print(len(df))
        if len(df)<100000:
            webhook('sql-server/operation_history', 'DADOS ABAIXO DE <100K')

            decorator_monitoria = stream_log_method_decorator_error(f'sql-server/operation_history')(execute_error)
            decorator_monitoria("Dados abaixo de 100k")
            
        return df
    except Exception as e:
        webhook('sql-server/operation_history', e)
        
    except exc.SQLAlchemyError as e:
        webhook('sql-server/operation_history', e)
                    
    except pyodbc.Error as e:
        webhook('sql-server/operation_history', e)


if __name__ == "__main__":
    
    print("Executanto procedure que replica operation_history")

    decorator_executor = stream_log_method_decorator('sql-server/operation_history')(main)    
    decorator_executor('proc_count_operation_history.sql')       