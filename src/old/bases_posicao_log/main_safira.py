
import datetime
import pandas as pd
import platform

from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import time

from conectores.conectar_sql_server import ConectSqlServer

from resourcex.utilitarios import Utils

#iniciar implementacao do envio kinesis - 06/10
#
#importar biblioteca importada pelo github: github.com/grupo-safira/geralog-monitoria.git
#
from geralog.decorator_log.monitoria import *


def extract_operations(database, hist_posicao_log, file):
    #exec_historico_posicaocontraparte_log
    @stream_log_method_decorator(f'sql-server/{hist_posicao_log}') 
    def execute():
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
    
    df = execute()
    
if __name__=='__main__':
    
    
    msg = f" BASES dbo.operation | 1/3 | PREPARAR EXTRACAO DE DADOS DOS THUNDERS E GERAR UAM BASE"
    print(datetime.datetime.now(),f"{msg.upper()}\n")
    
    tasks=[extract_operations('Book','exec_historico_posicao_log','proc_book_HistoricoPosicao_log.sql'),
           extract_operations('Book','exec_historico_posicaocontraparte_log','proc_book_HistoricoPosicaoContraParte_log.sql')]       
             
    with ProcessPoolExecutor(max_workers=10)  as executor:
        
        executor.map(tasks)
        executor.shutdown(wait=True)