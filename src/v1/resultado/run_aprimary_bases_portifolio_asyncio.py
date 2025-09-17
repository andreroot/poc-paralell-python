
import datetime
import pandas as pd
import platform
import asyncio
import aioodbc   

from v1.conectores.conectar_sql_server import ConectSqlServer
from v1.resourcex.utilitarios import Utils

#iniciar implementacao do envio kinesis - 06/10
#
#importar biblioteca importada pelo github: github.com/grupo-safira/geralog-monitoria.git
#
from geralog.decorator_log.monitoria_asyncio import *

async def extract_operations_v1(database, tag_monitoria, file):

    # Função assíncrona para executar consulta
    # async def extract_operations_v1(database, tag_monitoria, file):
    conect = ConectSqlServer()
    string_conn, conect_get_engine_pyodbc = conect.get_engine_pyodbc(database)
    # print(string_conn)

    utils = Utils()
    
    # asyncio.run(exec.main())
    so = platform.system()
    print(so)
    if so == 'Windows':
            
        script_proc = utils.read_file_win(file)
    else:
        script_proc = utils.read_file(file)  

    @stream_log_method_decorator(f'sql-server/{tag_monitoria}') 
    async def execute():              
        
        msg = f" BASES {database}.dbo.operation | 2/2 | EXECUTANDO QUERY NO SQL SERVER"
        print(datetime.datetime.now(),f"{msg.upper()}\n")
                  
        async with aioodbc.connect(dsn=string_conn) as conn:
            async with conn.cursor() as cursor:
                df = pd.DataFrame()
                # df = pd.read_sql_query(script_proc, get_engine_sqlalchemy)
                await cursor.execute(script_proc)
                rows = await cursor.fetchall()

                columns = [desc[0] for desc in cursor.description]  # Captura os nomes das colunas

                # Converte para DataFrame
                df = pd.DataFrame(rows, columns=columns)
        return df
    
    await execute()
    
if __name__=='__main__':
    
    
    msg = f" BASES dbo.operation | 1/2 | PREPARAR EXTRACAO DE DADOS DOS THUNDERS E GERAR UAM BASE"
    print(datetime.datetime.now(),f"{msg.upper()}\n")

    # Função principal para rodar múltiplas chamadas simultâneas
    async def main():
        procedures = [
            ("Book","extract_safira_all_operations","proc_base_safira_insert_all_history.sql"),  # Exemplo de procedure com parâmetros
            ("BookComercial","extract_comercial_all_operations","proc_base_comercial_insert_all_history.sql"),  # Outra procedure
            ("BookIndra","extract_indra_all_operations","proc_base_indra_insert_all_history.sql")
        ]

        tasks = [extract_operations_v1(database, tag_monitoria, file) for database, tag_monitoria, file in procedures]
        results = await asyncio.gather(*tasks)  # Executa simultaneamente
        return results


    asyncio.run(main())
