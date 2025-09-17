from prepared_table.insert_sql_server import InsertSqlServer

def get_columns_sql(database):
    """Retorna os execution logs de ate 7 dias atras para o qual o status e diferente de NULL"""

    # QUERY QUE RETORNA OS ERROS E PARTICULARIDADES DO ALERTA
    sql_alert = """
                SELECT --s.name as schema_name, t.name as table_name, 
                    c.name
                FROM sys.columns AS c
                INNER JOIN sys.tables AS t ON t.object_id = c.object_id
                INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
                WHERE t.name = 'Payments' AND s.name = 'APIpayments';
                """
    ins = InsertSqlServer()
    
    df = ins.execute_sql_from_df(sql_alert, database)

    lista_sql_col = df['name'].values.tolist()

    return lista_sql_col