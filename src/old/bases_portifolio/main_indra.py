
import datetime
# # import asyncio
# # import concurrent.futures
# import pandas as pd
from process_sql import ExecProcessSqlServerNew


if __name__=='__main__':
    
    exec = ExecProcessSqlServerNew()
    
    print(datetime.datetime.now())
    
    # Historico all book indra
    exec.extract_indra_all_operations('BookIndra','proc_base_indra_insert_all_history.sql')
    print(datetime.datetime.now())
