from conectores.conectar_sql_server import ConectSqlServer
    
from sqlalchemy import types
from sqlalchemy.exc import ProgrammingError

import numpy as np
import pandas as pd
import json
import datetime

from geralog.decorator_log.monitoria import *
def execute_error(e):
    return e


class InsertSqlServer:
    
    def __init__(self):
        pass

    # Mapeia tipos do Pandas para SQL Server
    def mapear_tipos(self, valor):
        if pd.isna(valor):  # Trata valores nulos
            return None
        elif isinstance(valor, int):
            return int(valor)
        elif isinstance(valor, float):
            return float(valor)
        elif isinstance(valor, bool):
            return bool(valor)
        elif isinstance(valor, pd.Timestamp): 
            return valor.strftime("%Y-%m-%d %H:%M:%S")  # Converte datetime para string
        elif isinstance(valor, datetime.date): #datetime64[D]
            return valor.strftime("%Y-%m-%d")  # Converte datetime para string
        else:
            return str(valor)  # Converte qualquer outro tipo para string

    def infer_sqlalchemy_dtype(self, series):
        """Infer SQLAlchemy data type from a pandas Series."""
        if pd.api.types.is_string_dtype(series):
            return types.String(length=255)  # Adjust length as needed
        elif pd.api.types.is_datetime64_any_dtype(series):
            return types.DateTime
        elif pd.api.types.is_numeric_dtype(series):
            if np.issubdtype(series.dtype, np.integer):
                return types.Integer
            elif np.issubdtype(series.dtype, np.float):
                return types.Float
        elif pd.api.types.is_boolean_dtype(series):
            return types.Boolean
        else:
            return types.String(length=255)  # Fallback to String if type is unknown
        
    def convert_column_to_datetime(self, df, columns_with_lists):
        """Convert a specified column to datetime."""
        try:
            for col in columns_with_lists:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                # print(f"Column '{col}' converted to datetime.")
        except Exception as e:
            print(f"Error converting column {col}: {e}")
        
        return df

    def convert_lists_to_text(self, df, columns_with_lists):
        """
        Convert columns containing lists to JSON strings.
        
        Parameters:
        - df (pd.DataFrame): The DataFrame with columns to convert.
        - columns_with_lists (list): List of column names that contain lists.
        
        Returns:
        - pd.DataFrame: DataFrame with columns containing lists converted to JSON strings.
        """
        for col in columns_with_lists:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)
        return df

    def execute_sql_from_df(self, sql_alert, database):
        """Gravar json na tabela via proc
        """
            
        # TRATAR O DATAFRAME RETORNO DA QUERY DO ALERTA
        df=pd.DataFrame()        
        try:
            conn = ConectSqlServer()

            df = pd.read_sql_query(sql_alert, conn.get_engine_sqlalchemy(database))
            
            return df
        
        except Exception as e:
            # Captura cualquier excepción de SQLAlchemy
            print(f"Error:{e}")
            return df
            
            
    # FUNCTION: CHAMDA PARA PROC QUE INSERE DADOS NA TABELA SQL
    def execute_sql(self, sql_query):

        """Gravar json na tabela via proc
        """

        try:

            conn = ConectSqlServer()
            
            # Create a SQLAlchemy engine
            engine = conn.get_engine_pyodbc()
            
            cursor = engine.cursor()
            
            cursor.fast_executemany = True
            
            cursor.execute(sql_query)
            
            engine.commit()

            print("Dados json gravado na tabela com sucesso!!!")

        except Exception as e:
            # Captura cualquier excepción de SQLAlchemy
            print(f"Error:{e}")        
            
    # FUNCTION: DELETE 
    def delete_from_table(self, df, database, schema, tab):
        
        conn = ConectSqlServer()
        
        # Create a SQLAlchemy engine
        engine = conn.get_engine_pyodbc()
                
        #Abre o Cursor
        cursor = engine.cursor()
        #Abre o Cursor
        # #cursor = self.engine.cursor()

        # creating column list for deletion
        cols_delete = ",".join(["'" + str(i) +"'" for i in df.id.tolist()])

        sql_Delete_query = " DELETE FROM ["+database+"].["+schema+"].["+tab+"] WHERE id IN (" + cols_delete + ") "
        
        cursor.fast_executemany = True
        cursor.execute(sql_Delete_query)
                
        #print(sql_Delete_query)
        engine.commit()


    # FUNCTION: EXISTING DELETE 
    def exists_from_table_all(self, schema, tab):
        

        sql_exists = f""" SELECT COUNT(1)
                            FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_NAME = '{tab}'
                            AND TABLE_SCHEMA = '{schema}' 
                            """
        
        df = self.execute_sql_from_df(sql_exists, 'Treinamento')

        print(df.values.tolist())

        return df.values.tolist()

    # FUNCTION: DELETE 
    def delete_from_table_all(self, df, database, schema, tab):
        
        conn = ConectSqlServer()
        
        # Create a SQLAlchemy engine
        engine = conn.get_engine_pyodbc()
                
        #Abre o Cursor
        cursor = engine.cursor()
        #Abre o Cursor
        # #cursor = self.engine.cursor()

        # creating column list for deletion
        # cols_delete = ",".join(["'" + str(i) +"'" for i in df.id.tolist()])

        sql_Delete_query = " DELETE FROM ["+database+"].["+schema+"].["+tab+"] WHERE 1=1 "
        
        cursor.fast_executemany = True
        cursor.execute(sql_Delete_query)
                
        #print(sql_Delete_query)
        engine.commit()
        
   # FUNCTION: DELETE 
    def delete_from_table_finance(self, df, database, schema, tab):
        
        conn = ConectSqlServer()
        
        # Create a SQLAlchemy engine
        engine = conn.get_engine_pyodbc()
                
        #Abre o Cursor
        cursor = engine.cursor()
        #Abre o Cursor
        # #cursor = self.engine.cursor()

        # creating column list for deletion
        cols_delete = ",".join(["'" + str(i) +"'" for i in df.orderId.tolist()])

        sql_Delete_query = " DELETE FROM ["+database+"].["+schema+"].["+tab+"] WHERE orderId IN (" + cols_delete + ") "
        
        cursor.fast_executemany = True
        cursor.execute(sql_Delete_query)
                
        #print(sql_Delete_query)
        engine.commit()


   # FUNCTION: DELETE 
    def delete_from_table_juridic_incremental(self, df, database, schema, tab):
        
        conn = ConectSqlServer()
        
        # Create a SQLAlchemy engine
        engine = conn.get_engine_pyodbc()
                
        #Abre o Cursor
        cursor = engine.cursor()
        #Abre o Cursor
        # #cursor = self.engine.cursor()

        # creating column list for deletion
        cols_delete = ",".join(["'" + str(i) +"'" for i in df.currentContractCode.tolist()])

        sql_Delete_query = " DELETE FROM ["+database+"].["+schema+"].["+tab+"] WHERE currentContractCode IN (" + cols_delete + ") "
        
        cursor.fast_executemany = True
        cursor.execute(sql_Delete_query)
                
        #print(sql_Delete_query)
        engine.commit()
                   
    # insert via script sql : primeiro delete from e depois insert into
    def insert_sql_from_table(self, df, database, schema, tab):
        try:
            conn = ConectSqlServer()
            
            # Create a SQLAlchemy engine
            engine = conn.get_engine_pyodbc()
                    
            #Abre o Cursor
            cursor = engine.cursor()
            #Abre o Cursor
            # #cursor = self.engine.cursor()

            df['DataInsertTable']=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            
            # creating column list for deletion
            # cols = ",".join(["'" + str(i) +"'" for i in df.id.tolist()])

            insert_sql = f'INSERT INTO {database}.{schema}.{tab} ({", ".join([f"[{col}]" for col in df.columns.tolist()])}) VALUES ({", ".join(["?"] * len(df.columns.tolist()))})'
            
            insert_values = [tuple(self.mapear_tipos(col) for col in row) for row in df.itertuples(index=False)]
            # list(map(list, df.itertuples(index=False)))
            #print(insert_values)
            cursor.fast_executemany = True
            cursor.executemany(insert_sql,  insert_values)
            
            #print(sql_Delete_query)
            engine.commit()
        except ProgrammingError:
            #if there's the error regarding table syntax, write to the log and continue whatever it's doing
            print("Failed table {} creation!".format(tab))

    # type_insert: append | replace
    def insert_dataframe_from_table(self, df, database, schema, tab, type_insert, dtype=None):
        
        conn = ConectSqlServer()
        
        # Create a SQLAlchemy engine
        engine = conn.get_engine_sqlalchemy(database)
        
        df['DataInsertTable']=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        def calculate_chunksize(df, max_params=1900):
            """Calcula o tamanho ideal do chunksize para df.to_sql"""
            num_columns = len(df.columns)
            return max(1, max_params // num_columns)  # Garante ao menos 1 linha por batch
        
        # Calcula chunksize ideal
        chunksize = calculate_chunksize(df)
                        
        # # Infer schema if dtype is not provided
        # if dtype is None:
        #     dtype = {col: self.infer_sqlalchemy_dtype(df[col]) for col in df.columns}
        try:
            # # Loop para inserir em partes se for type_insert=append
            # for i in range(0, len(df), chunksize):
            #     df_chunk = df.iloc[i:i+chunksize]
            with engine.begin() as con:
                df.to_sql(
                    con=con,
                    schema= schema,
                    name= tab,
                    if_exists=type_insert,
                    index=False,
                    chunksize=chunksize,
                    method="multi",
                    dtype=dtype
                )
            
            engine.dispose()
        
            print('Done inserting data on SQL Server.')
        except Exception as e:
            print(f"An error occurred while sending data to SQL: {e}")                
            # webhook(tag_monitoria, e)
            decorator_monitoria = stream_log_method_decorator_error(f'dados/api-thunders/{tab}')(execute_error)
            decorator_monitoria(f'An error occurred while sending data to SQL: {e}')                      
