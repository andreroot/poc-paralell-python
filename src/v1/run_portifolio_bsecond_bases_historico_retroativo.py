
import datetime
from datetime import timedelta
import pandas as pd
import platform

from sqlalchemy import  text

from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

from conectores.conectar_sql_server import ConectSqlServer
from resourcex.utilitarios import Utils

#iniciar implementacao do envio kinesis - 06/10
#
#importar biblioteca importada pelo github: github.com/grupo-safira/geralog-monitoria.git
#
from geralog.decorator_log.monitoria import *

def get_param(database):
    # EXECUTA SELECT COUNT file2
    
    conn = ConectSqlServer()
    get_engine_sqlalchemy = conn.get_engine_sqlalchemy(database)
    
    # PARAMETRO 1
    query_data="""
			SELECT DISTINCT
				[Data] data
			FROM Book.Curva.Curva_FWD
			WHERE Curva = 'Oficial'
			AND [Data] >= (SELECT MIN(DataHistorico_d1) FROM Modelo.[POC_Historico].[Diferencas_agg])
    """ 
               
    df = pd.DataFrame()
    df = pd.read_sql_query(query_data, get_engine_sqlalchemy)
    # # print(datetime.datetime.strptime("", "%Y-%m-%d"))
    # print(datetime.datetime.now().strftime("%Y-%m-%d"))
    # print(datetime.date.today()+datetime.timedelta(days=10))

    df1 = df[df['data']>=datetime.date.today()+datetime.timedelta(days=-2)].copy()
    df1['data_0'] = df1['data'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d'))
    df1['data_1'] = df1['data'].apply(lambda x: (pd.to_datetime(x)+datetime.timedelta(days=1)).strftime('%Y-%m-%d'))

    
    # PARAMETRO 2
    query_vpl="""select top 1 vpl from treinamento.gpires.tabelavpl where dateInsert = (SELECT MAX(dateInsert) FROM treinamento.gpires.tabelavpl)"""

    df2 = pd.DataFrame()
    df2 = pd.read_sql_query(query_vpl, get_engine_sqlalchemy)    
    param_vpl = df2['vpl'].tolist()

    df1['vpl'] = str(param_vpl[0])
    
    # parametros= df1.merge(df2,on='data', how='outer') #pd.concat([df1, df2], ignore_index=True)
    
    return df1[['vpl','data_0','data_1']]

def extract_operations(database, file, file2):
    
    conn = ConectSqlServer()
    engine = conn.get_engine_sqlalchemy(database)
        
    msg = f" BASES  | 2/2 | EXECUTANDO QUERY NO SQL SERVER"
    print(datetime.datetime.now(),f"{msg.upper()}\n")
    
    # vpl_list = [1000, 2000, 3000]  # Exemplo de diferentes valores para vpl
    # datas_d0 = ['2024-01-01', '2024-02-01', '2024-03-01']
    # datas_d1 = ['2024-01-31', '2024-02-28', '2024-03-31']

    # params = get_param("Book")
    # vpl_list = params['vpl'].tolist()
    # datas_d0 = params['data_0'].tolist()
    # datas_d0 = params['data_1'].tolist()
        
    # print(vpl_list)
    # print(datas_d0)
    # print(datas_d1)
    
    # # # Loop para executar a procedure com diferentes valores
    # with engine.connect() as conn:
    #     for vpl, data_d0, data_d1 in zip(vpl_list, datas_d0, datas_d1):
    #         sql = text("""
    #         EXEC Modelo.dbo.STP_gera_bases_POC_Historico 
    #             vpl=:vpl,
    #             data_d0=:data_d0,
    #             data_d1=:data_d1,
    #             curva1='Oficial',
    #             curva2='Oficial',
    #             anofornecimento_min=2024;

    #         """)

    #         # Executando a procedure com parâmetros dinâmicos
    #         conn.execute(sql, {"vpl": vpl, "data_d0": data_d0, "data_d1": data_d1})
    #         conn.commit()  # Confirma a transação
                    
    #         print(f"Executado para vpl={vpl}, data_d0={data_d0}, data_d1={data_d1}")

    params = get_param("Book")

    # Convertendo para lista de tuplas
    params = [tuple(p) for p in params.values.tolist()]
    print(params)
    # connectionString, engine = conn.get_engine_pyodbc(database)
    # with engine.cursor() as cursor:
    #     sql = """
    #         EXEC Modelo.dbo.STP_gera_bases_POC_Historico 
    #             vpl=?,
    #             data_d0=?,
    #             data_d1=?,
    #             curva1='Oficial',
    #             curva2='Oficial',
    #             anofornecimento_min=2024;

    #     """
    #     cursor.fast_executemany=True
    #     # Executando a procedure com parâmetros dinâmicos
    #     cursor.executemany(sql, params)
    #     engine.commit()  # Confirma a transação
    # cursor.close()
    # engine.close()
                               
    print("Execução finalizada!")
        
    # # EXECUTA SELECT COUNT file2
    # utils = Utils()

    # get_engine_sqlalchemy = conn.get_engine_sqlalchemy(database)
    
    # so = platform.system()
    # if so == 'Windows':
            
    #     sql_query = utils.read_file_win(file2)
    # else:
    #     sql_query = utils.read_file(file2)  
               
    # df = pd.DataFrame()
    # df = pd.read_sql_query(sql_query, get_engine_sqlalchemy)
    
    # return df
    
if __name__=='__main__':
    
    
    msg = f" BASES [modelo].[BaseHistorica].[BoletasProcessadasv2] | 1/3 | GERAR BASES HISTORICOS DE RESULTADO"
    print(datetime.datetime.now(),f"{msg.upper()}\n")
    
    # decorator_executor = stream_log_method_decorator('sql-server/poc_historico')(extract_operations)
    
    extract_operations("Book","proc_STP_gera_bases_POC_Historico_portifolio.sql","proc_count_STP_gera_bases_POC_Historico_portifolio.sql")       

    # paramteros = get_param("Book")
    # print(paramteros.values.tolist())
    # print(datas_d0)
    # print(datas_d1)