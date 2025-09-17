
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

    @stream_log_method_decorator('sql-server/exec_historico_posicao_log')        
    def exec_historico_posicao_log(self, database, file):

        conn = ConectSqlServer()
        utils = Utils()
        
        # asyncio.run(exec.main())
        so = platform.system()
        print(so)
        if so == 'Windows':
                
            script_proc = utils.read_file_win(file)
        else:
            script_proc = utils.read_file(file) 
        
        #print(script_proc)
                        
        get_engine_sqlalchemy = conn.get_engine_sqlalchemy(database)

                
        df = pd.DataFrame()
        df = pd.read_sql_query(script_proc, get_engine_sqlalchemy)

        return df

    @stream_log_method_decorator('sql-server/exec_historico_resultado_log')   
    def exec_historico_resultado_log(self, database, file):

        conn = ConectSqlServer()
        utils = Utils()
        
        # asyncio.run(exec.main())
        so = platform.system()
        print(so)
        if so == 'Windows':
                
            script_proc = utils.read_file_win(file)
        else:
            script_proc = utils.read_file(file) 

        get_engine_sqlalchemy = conn.get_engine_sqlalchemy(database)

                
        df = pd.DataFrame()
        df = pd.read_sql_query(script_proc, get_engine_sqlalchemy)

        return df
    
    @stream_log_method_decorator('sql-server/exec_historico_posicaocontraparte_log')        
    def exec_historico_posicaocontraparte_log(self, database, file):

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

    @stream_log_method_decorator('sql-server/exec_informacao_comercial')        
    def exec_informacao_comercial(self, database, file):

        conn = ConectSqlServer()
        
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
    
    def valida_execucao_manual_via_notebook(self, database, query):

        conn = ConectSqlServer()
        
        ## Querys para Selecionar o Portfólio Histórico
        # script_proc =(f"""  """)

        get_engine_sqlalchemy = conn.get_engine_sqlalchemy(database)

                
        df = pd.DataFrame()
        df = pd.read_sql_query(query, get_engine_sqlalchemy)
                
        return df
            
    # async def main(self):
    #     process_01_task = asyncio.create_task(self.exec_temp_book_all_operations('Book','src/sql/proc_book_all_history.sql'))
    #     process_02_task = asyncio.create_task(self.exec_temp_bookcomercial_all_operations('BookComercial','src/sql/proc_bookcomercial_all_history.sql')])
    #     await process_02_task
    #     try:
    #         await process_task
    #     except asyncio.CancelledError:
    #         print("main(): Processo erro")

# if __name__=='__main__':
    
#     exec = ExecProcessSqlServer()
    
#     print(datetime.datetime.now())
    
#     # asyncio.run(exec.main())

#     # Historico all book
#     exec.exec_temp_book_all_operations('Book','src/sql/proc_book_all_history.sql')
    
#     # Historico all book comercial
#     exec.exec_temp_bookcomercial_all_operations('BookComercial','src/sql/proc_bookcomercial_all_history.sql')
    
#     # Historico all book indra
#     exec.exec_temp_bookindra_all_operations('BookIndra','src/sql/proc_bookindra_all_history.sql')
    
#     print(datetime.datetime.now())
