import logging
import sys
from pathlib import Path
import time

import pandas as pd
import datetime

import sqlalchemy
from geralog.decorator_log.put import stream_log_method_decorator

sys.path.append((Path(__file__).parent.parent).resolve().as_posix())

from conectores.conectar_sql_server import ConectSqlServer

logging.basicConfig()
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)


@stream_log_method_decorator(f"sql-server/poc_bi_historico")
def main_poc_bi_historico():
    conn = ConectSqlServer()
    
    msg = f" EXECUTANDO  STP_gera_BI_POC_Historico | Modelo.[POC_Historico].[proc_POC_Historico_diff_BI] | EXECUTANDO QUERY NO SQL SERVER"
    print(datetime.datetime.now(),f"{msg.upper()}\n")
                        
    get_engine_sqlalchemy = conn.get_engine_sqlalchemy('Modelo')
    
    df = pd.DataFrame()
    df = pd.read_sql_query(
            """
			BEGIN
				IF OBJECT_ID('tempdb..#TempBoletas') IS NOT NULL
				BEGIN
					DROP TABLE #TempBoletas;
				END

				SELECT * INTO #TempBoletas
				FROM (
						SELECT * 
						FROM Modelo.dbo.proc_POC_Historico_diff_BI
						) TempBoletas;
				

				SELECT * FROM #TempBoletas;

			END;


        """, get_engine_sqlalchemy)
    
    # print(len(df))
    return df


if __name__ == "__main__":
    
    main_poc_bi_historico()
    