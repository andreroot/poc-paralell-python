import logging
import sys
from pathlib import Path
import time

#import sqlalchemy
from geralog.decorator_log.put import stream_log_method_decorator

sys.path.append((Path(__file__).parent.parent).resolve().as_posix())

#from connectors.sql_server import ConnectSqlServer
from resourcex.utilitarios import Utils

logging.basicConfig()
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)


@stream_log_method_decorator(f"sql-server/poc_historico")
def main():
    
    # modelo_engine = ConnectSqlServer().get_engine(
    #     "Modelo",
    #     fast_executemany=False,
    #     connect_args={"connect_timeout": 300, "echo": True},
    # )
    
    print("Salvando comparações em tabela histórico agregado")
    
    utils = Utils()
    script_sql = utils.read_file('sql/poc_historico.sql')

    print(script_sql)
    
    # with modelo_engine.begin() as conn:
    #     conn.execute( sqlalchemy.text(script_sql) )

    time.sleep(60)

    print("Alimentando tabela diff para BI")
    
    utils = Utils()
    script_sql = utils.read_file('sql/poc_historico.sql')
    
    print(script_sql)
    # with modelo_engine.begin() as conn:
    #     conn.execute(  sqlalchemy.text() )


if __name__ == "__main__":
    main()
