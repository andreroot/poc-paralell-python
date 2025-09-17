# import pyodbc
# import sqlalchemy
# from sqlalchemy.engine import URL

# import datetime
# # from datetime import datetime

# import json
# import os
# import sys

# #buscar credencial que esta gravda dentro secret do aws
# def get_secret_sql():
    
#     from conectores.conectar_aws import ConectAWS
#     from botocore.exceptions import ClientError

#     cred = ConectAWS() 
#     awssecret = cred.get_svc_user_credentials()

#     region = os.getenv("AWS_DEFAULT_REGION")
#     secret_name = "acesso-sql-server"

#     client = awssecret.client(
#         service_name='secretsmanager',
#         region_name=region 
#     )

#     try:
#         get_secret_value_response = client.get_secret_value(
#             SecretId=secret_name
#         )
#     except ClientError as e:
#         # For a list of exceptions thrown, see
#         # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
#         raise e

#     # Decrypts secret using the associated KMS key.
#     secret = get_secret_value_response['SecretString']

#     # Your code goes here.

#     return json.loads(secret)

# class ConectSqlServer:
        
#     def __init__(self):
#         print('buscar credencial na aws')
        
#         xpass = get_secret_sql()
#         self.USERNAME = xpass['usuario']
#         self.PASSWORD = xpass['senha']
#         self.SERVER = '172.16.10.4'
#         self.DATABASE = 'Book'

#     def get_odbc_driver(self) -> str:
#         """Returns first DRIVER with name starting with ODBC Driver from
#         pyodbc drivers list.
#         """
#         drivers = [
#             driver for driver in pyodbc.drivers() if driver.startswith("ODBC Driver")
#         ]
#         driver = drivers[0]
#         return driver
    
#     def get_engine_pyodbc(self, databse):

#         print('Creating engine...')
#         # drivers = [item for item in pyodbc.drivers()]

#         # for driver in drivers:
#         #     print(driver)
#         #     # Check for correct driver 
#         #     if 'ODBC' in driver and 'SQL Server' in driver:
#         #         use_driver = driver #ODBC Driver 18 for SQL Server

#         use_driver = self.get_odbc_driver()
#         print(f"Driver SQL: {use_driver!r}")

#         SERVER = self.SERVER
#         # DATABASE = self.DATABASE
#         USERNAME = self.USERNAME
#         PASSWORD = self.PASSWORD

#         #print(self.USERNAME)
#         #print(self.PASSWORD)
        
        
#         connectionString = 'DRIVER={'+f'{use_driver}'+'};'+f'SERVER={SERVER};DATABASE={databse};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes'

#         print(f"conectado as {datetime.datetime.now()}")
        
#         engine = pyodbc.connect(connectionString) 
#         #https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-setup-tools?view=sql-server-ver15&tabs=ubuntu-install
#         #
#         return connectionString, engine


#     def get_engine_sqlalchemy(self, database: str = None, **kwargs) -> sqlalchemy.engine.base.Engine:

#         print('Creating engine - sqlalchemy...')

#         use_driver = self.get_odbc_driver()
#         print(f"Driver SQL: {use_driver!r}")

#         connect_url = URL.create(
#             "mssql+pyodbc",
#             username=self.USERNAME,
#             password=self.PASSWORD,
#             host='172.16.10.4',
#             port='1433',
#             database=database if database else 'Book',
#             query={
#                 "driver": use_driver,
#                 "UseFMTONLY": "yes",
#                 "TrustServerCertificate": "yes",
#             },
#         )

#         print(f"conectado as {datetime.datetime.now()}")

#         engine = sqlalchemy.create_engine(connect_url, **kwargs, fast_executemany=True)
#         return engine
    
