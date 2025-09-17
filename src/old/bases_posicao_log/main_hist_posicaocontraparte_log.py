
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

@stream_log_method_decorator('sql-server/exec_historico_posicaocontraparte_log')        
def execute( database, file):

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
    
if __name__=='__main__':
        
    print(datetime.datetime.now())

    # Historico Posição Contraparte
    execute('Book','proc_book_HistoricoPosicaoContraParte_log.sql')  
            
    print(datetime.datetime.now())
