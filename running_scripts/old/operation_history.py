import sys
from pathlib import Path


import sqlalchemy

from geralog.decorator_log.put import stream_log_method_decorator

sys.path.append((Path(__file__).parent.parent).resolve().as_posix())

from connectors.sql_server import ConnectSqlServer

@stream_log_method_decorator(f"sql-server/operation_history")
def main():
    modelo_engine = ConnectSqlServer().get_engine(
        "Modelo",
        fast_executemany=False,
        connect_args={"connect_timeout": 60},
    )
    print("Executanto procedure que replica operation_history")
    with modelo_engine.begin() as conn:
        conn.execute(sqlalchemy.text(
            "EXEC dbo.STP_operation_history"
        ))

if __name__ == "__main__":
    main()