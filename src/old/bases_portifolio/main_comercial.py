
import datetime
# # import asyncio
# # import concurrent.futures
# import pandas as pd
from process_sql import ExecProcessSqlServerNew
import platform

if __name__=='__main__':
    
    exec = ExecProcessSqlServerNew()
    
    print(datetime.datetime.now())
    
    exec.extract_comercial_all_operations('BookComercial','proc_base_comercial_insert_all_history.sql')        
    print(datetime.datetime.now())
