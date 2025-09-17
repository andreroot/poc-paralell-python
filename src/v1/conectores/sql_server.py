import json
import os
from dataclasses import dataclass
from typing import Literal

import pandas as pd
import pyodbc
import sqlalchemy
from sqlalchemy.engine import URL


def get_secret_sql():
    from conectores.aws import ConnectAWS

    svc_user_boto_session = ConnectAWS().get_svc_user_boto_session()
    secrets_manager_client = svc_user_boto_session.client(
        service_name="secretsmanager", region_name=os.getenv("AWS_DEFAULT_REGION")
    )

    secret_id = "acesso-sql-server"
    get_secret_value_response = secrets_manager_client.get_secret_value(
        SecretId=secret_id
    )

    return json.loads(get_secret_value_response["SecretString"])


def get_odbc_driver() -> str:
    """Retorna o primeiro driver ODBC encontrado"""
    drivers = [
        driver
        for driver in pyodbc.drivers()
        if (("ODBC" in driver) and ("SQL Server" in driver))
    ]
    driver = drivers[0]
    return driver


@dataclass(frozen=True)
class SqlServerCredentias:
    username: str
    password: str
    server: str
    port: str
    database: str


class ConnectSqlServer:
    def __init__(self):
        xpass = get_secret_sql()
        self.credentials = SqlServerCredentias(
            username=xpass["usuario"],
            password=xpass["senha"],
            server="172.16.10.4",
            port="1433",
            database="Book",
        )

    def get_engine(
        self, database: str = None, **kwargs
    ) -> sqlalchemy.engine.base.Engine:
        use_driver = get_odbc_driver()
        print(f"Driver SQL: {use_driver!r}")
        connect_url = URL.create(
            "mssql+pyodbc",
            username=self.credentials.username,
            password=self.credentials.password,
            host=self.credentials.server,
            port=self.credentials.port,
            database=database if database else self.credentials.database,
            query={
                "driver": use_driver,
                "UseFMTONLY": "yes",
                "TrustServerCertificate": "yes",
            },
        )
        engine = sqlalchemy.create_engine(connect_url, **kwargs)
        return engine


def upsert_table_pipeline(
    df: pd.DataFrame,
    conn: sqlalchemy.engine.base.Connection,
    table: str,
    schema: str,
    pipeline: str,
    if_not_exists: Literal["raise", "ignore"] = "raise",
) -> int | None:
    if (
        not conn.dialect.has_table(conn, table, schema=schema)
    ) and if_not_exists == "raise":
        raise ValueError(f"Table [{schema}].[{table}] doesn't exist!")

    with open(
        f"src/pipelines/{pipeline}/ddl/{table}.sql",
        "r",
        encoding="utf-8",
    ) as file:
        temp_table_sql = file.read()

    with open(
        f"src/pipelines/{pipeline}/dml/upsert_{table}.sql",
        "r",
        encoding="utf-8",
    ) as file:
        upsert_table_sql = file.read()

    conn.execute(sqlalchemy.text(temp_table_sql))
    df.to_sql(f"#{table}", conn, if_exists="append", index=False)
    conn.execute(sqlalchemy.text(upsert_table_sql.format(table=table, schema=schema)))
    df.to_sql(
        table,
        conn,
        schema=schema,
        if_exists="append",
        index=False,
    )
    return df.shape[0]


def upsert_table_pipeline_json(
    df: pd.DataFrame,
    conn: sqlalchemy.engine.base.Connection,
    table: str,
    schema: str,
    pipeline: str,
    if_not_exists: Literal["raise", "ignore"] = "raise",
) -> int | None:
    if (
        not conn.dialect.has_table(conn, table, schema=schema)
    ) and if_not_exists == "raise":
        raise ValueError(f"Table [{schema}].[{table}] doesn't exist!")

    with open(
        f"src/pipelines/{pipeline}/ddl/{table}.sql",
        "r",
        encoding="utf-8",
    ) as file:
        temp_table_sql = file.read()

    with open(
        f"src/pipelines/{pipeline}/dml/upsert_{table}.sql",
        "r",
        encoding="utf-8",
    ) as file:
        upsert_table_sql = file.read()

    with open(
        f"src/pipelines/{pipeline}/dml/insert_json_{table}.sql",
        "r",
        encoding="utf-8",
    ) as file:
        insert_table_sql = file.read()

    # create temp table
    conn.execute(sqlalchemy.text(temp_table_sql))

    # insert into temp table
    conn.execute(
        sqlalchemy.text(insert_table_sql.format(target_table=f"#{table}")),
        parameters={
            "batch_json": df.to_json(
                orient="records", date_format="iso", force_ascii=False
            )
        },
    )

    # delete from target table
    conn.execute(sqlalchemy.text(upsert_table_sql.format(table=table, schema=schema)))

    # insert from temp into target table
    conn.execute(
        sqlalchemy.text(insert_table_sql.format(target_table=f"[{schema}].[{table}]")),
        parameters={
            "batch_json": df.to_json(
                orient="records", date_format="iso", force_ascii=False
            )
        },
    )
    return df.shape[0]


def insert_dataframe(
    dataframe: pd.DataFrame,
    table: str,
    connection: sqlalchemy.engine.base.Connection,
    schema: str,
    if_not_exists: Literal["raise", "ignore"] = "raise",
    **to_sql_kwargs,
) -> None:
    if (
        not connection.dialect.has_table(connection, table, schema=schema)
    ) and if_not_exists == "raise":
        raise ValueError(f"Table [{schema}].[{table}] doesn't exist!")
    dataframe.to_sql(table, connection, schema=schema, **to_sql_kwargs)


def execute_procedure_pipeline(
    conn: sqlalchemy.engine.base.Connection, procedure: str, pipeline: str, **params
):
    with open(
        f"src/pipelines/{pipeline}/procedures/{procedure}.sql",
        "r",
        encoding="utf-8",
    ) as file:
        procedure_sql = file.read()
    conn.execute(sqlalchemy.text(procedure_sql), params)
