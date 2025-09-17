from conectores.conectar_sql_server import ConectSqlServer
    
from sqlalchemy import types
import numpy as np
import pandas as pd
import json
import datetime

class InsertSqlServer:
    
    def __init__(self):
        pass


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
                print(f"Column '{col}' converted to datetime.")
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
            
    def send_df_to_sql_with_schema(self, df, schema, table_name,  database, dtype=None):
        
        conn = ConectSqlServer()
        
        # Create a SQLAlchemy engine
        engine = conn.get_engine_sqlalchemy(database)
        
        df['DataInsertTable']=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        # # Infer schema if dtype is not provided
        # if dtype is None:
        #     dtype = {col: self.infer_sqlalchemy_dtype(df[col]) for col in df.columns}
        try:
                    # Create the table if schema is provided
            # if dtype:
            #     create_table_if_not_exists(engine, table_name, dtype)
            with engine.begin() as con:
                df.to_sql(
                    con=con,
                    schema= schema,
                    name= table_name,
                    if_exists='replace',
                    index=False,
                    chunksize=1000,
                    dtype=dtype
                )
        
            print('Done inserting data on SQL Server.')
        except Exception as e:
            print(f"An error occurred while sending data to SQL: {e}")            