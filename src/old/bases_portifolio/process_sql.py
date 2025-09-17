
import datetime
# import asyncio
# import concurrent.futures
import pandas as pd
import platform


from conectores.conectar_sql_server import ConectSqlServer

from resourcex.utilitarios import Utils

#iniciar implementacao do envio kinesis - 06/10
#
#importar biblioteca importada pelo github: github.com/grupo-safira/geralog-monitoria.git
#
from geralog.decorator_log.monitoria import *
# from geralog.decorator_log.put import stream_log_method_decorator

# def conn():
#     conn = ConectSqlServer()

#     self.get_engine_pyodbc = conn.get_engine_pyodbc()
#     self.get_engine_sqlalchemy = conn.get_engine_sqlalchemy()

class ExecProcessSqlServerNew:
    
    def __init__(self):
        pass

    @stream_log_method_decorator('sql-server/extract_safira_all_operations') 
    def extract_safira_all_operations(self, database, file):

        conn = ConectSqlServer()
        
        utils = Utils()
        
        # asyncio.run(exec.main())
        so = platform.system()
        print(so)
        if so == 'Windows':
                
            script_proc = utils.read_file_win(file)
        else:
            script_proc = utils.read_file(file)  
        
        # print(script_proc)
                        
        get_engine_sqlalchemy = conn.get_engine_sqlalchemy(database)

                
        df = pd.DataFrame()
        df = pd.read_sql_query(script_proc, get_engine_sqlalchemy)

        return df

    @stream_log_method_decorator('sql-server/extract_comercial_all_operations') 
    def extract_comercial_all_operations(self, database, file):

        conn = ConectSqlServer()
        
        utils = Utils()
        
        # asyncio.run(exec.main())
        so = platform.system()
        print(so)
        if so == 'Windows':
                
            script_proc = utils.read_file_win(file)
        else:
            script_proc = utils.read_file(file) 
        # print(script_proc)
                        
        get_engine_sqlalchemy = conn.get_engine_sqlalchemy(database)

                
        df = pd.DataFrame()
        df = pd.read_sql_query(script_proc, get_engine_sqlalchemy)

        return df
        
    @stream_log_method_decorator('sql-server/extract_indra_all_operations') 
    def extract_indra_all_operations(self, database, file):

        conn = ConectSqlServer()

        utils = Utils()
        
        # asyncio.run(exec.main())
        so = platform.system()
        print(so)
        if so == 'Windows':
                
            script_proc = utils.read_file_win(file)
        else:
            script_proc = utils.read_file(file)          
        # print(script_proc)
                        
        get_engine_sqlalchemy = conn.get_engine_sqlalchemy(database)

                
        df = pd.DataFrame()
        df = pd.read_sql_query(script_proc, get_engine_sqlalchemy)

        return df
