from prepared_table.insert_sql_server import InsertSqlServer
from sqlalchemy import types
from conectores.conectar_sql_server import ConectSqlServer
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

import pandas as pd
import time
import datetime

class PreparedSchemaTableMeasuringParalell:
    
    def __init__(self):
        pass
        # self.database = 'TREINAMENTO'
        # self.schema = 'lkok'
    
    # INSERÇÃO VIA SCRIPT SQL DELETE FROM E DEPOIS INSERT INTO
    def prepared_sql_from_table(self, df, database, schema, tab):
        
        # função para execução de isntrução sql
        ins = InsertSqlServer()
        
        if tab=='MeasuringAdjust':
            
            #Substituir Valores NaN por None
            df = df.where(pd.notnull(df), None)
            
            # ETAPA DELEÇÃO: DELETE FROM
            ins.delete_from_table(df, database, schema, tab)

            # Convert list columns to JSON strings
            columns_with_lists = ['period']
            
            df = ins.convert_column_to_datetime(df, columns_with_lists)

            # ETAPA INSERÇÃO: INSERT INTO
            ins.insert_sql_from_table(df, database, schema, tab) #df, database, schema, tab)

        elif tab=='MeasuringProjectionConsolidateMonthYear':

            # Substituir Valores NaN por None
            df = df.where(pd.notnull(df), None)

            # ETAPA DELEÇÃO: DELETE FROM
            ins.delete_from_table(df, database, schema, tab)
                                    
            columns_with_lists = ['period']
            
            df = ins.convert_column_to_datetime(df, columns_with_lists)

           # ETAPA INSERÇÃO: INSERT INTO
            ins.insert_sql_from_table(df, database, schema, tab) #df, database, schema, tab)
            
        elif tab=='MeasuringAdjustConsolidated':

            # Substituir Valores NaN por None
            df = df.where(pd.notnull(df), None)

            # ETAPA DELEÇÃO: DELETE FROM
            ins.delete_from_table(df, database, schema, tab)
                                    
            columns_with_lists = ['reference']
            
            df = ins.convert_column_to_datetime(df, columns_with_lists)

            # ETAPA INSERÇÃO: INSERT INTO
            ins.insert_sql_from_table(df, database, schema, tab) #df, database, schema, tab)
                                
                                                
    # PROCESSO EM TESTES INSERÇÃO VIA DATAFRAME: APPEND OU REPLACE
    def prepared_dataframe_from_table(self, df, database, schema, tab):
        
        # função para execução de isntrução sql
        ins = InsertSqlServer()
        
        if tab=='MeasuringAdjust':
            
            #Substituir Valores NaN por None
            df = df.where(pd.notnull(df), None)
            
            # ETAPA DELEÇÃO: DELETE FROM
            # ins.delete_from_table_all(df, database, schema, tab)
                        
            forced_dtype = {
                    'id': types.String(length=255),
                    'measuringPointId': types.String(length=255),
                    'physicalAssetId': types.String(length=255),
                    'isCompleted': types.Boolean,
                    'isZero': types.Boolean,
                    'period': types.DateTime,
                    'activeGeneration': types.Float,
                    'activeConsumption': types.Float,
                    'reactiveGeneration': types.Float,
                    'reactiveConsumption': types.Float
                    
                    }
            # Convert list columns to JSON strings
            columns_with_lists = ['period']
            
            df = ins.convert_column_to_datetime(df, columns_with_lists)

            # ETAPA INSERÇÃO: INSERT INTO
            ins.insert_dataframe_from_table(df, database, schema, tab, 'append', forced_dtype)

        elif tab=='MeasuringProjectionConsolidateMonthYear':

            # Substituir Valores NaN por None
            df = df.where(pd.notnull(df), None)
            
            # ins.delete_from_table_all(df, database, schema, tab)
                        
            forced_dtype = {
                'period': types.DateTime,
                'hoursInPeriod': types.Integer,
                'id': types.String(length=100),
                'consumptionPeak.projected': types.Float,
                'consumptionPeak.adjusted': types.Float,
                'consumptionPeakMwm.projected': types.Float,
                'consumptionOffPeak.projected': types.Float,
                'consumptionOffPeakMwm.projected': types.Float,
                'activeConsumption.projected': types.Float,
                'activeConsumptionMwm.projected': types.Float,
                'reactiveConsumption.projected': types.Float,
                'reactiveConsumptionMwm.projected': types.Float,
                'demandPeak.projected': types.Float,
                'demandPeakMwm.projected': types.Float,
                'demandOffPeak.projected': types.Float,
                'demandOffPeakMwm.projected': types.Float,
            }
            
            columns_with_lists = ['period']
            
            df = ins.convert_column_to_datetime(df, columns_with_lists)

            ins.insert_dataframe_from_table(df, database, schema, tab, 'append', forced_dtype)


        elif tab=='MeasuringAdjustConsolidated':

            # Substituir Valores NaN por None
            df = df.where(pd.notnull(df), None)
            
            # ins.delete_from_table_all(df, database, schema, tab)
                                    
            forced_dtype = {
                'reference': types.DateTime,
                'id': types.String(length=100),    
                'consumptionPeak.measured': types.Float,
                'consumptionOffPeak.measured': types.Float,
                'demandPeak.measured': types.Float,
                'demandOffPeak.measured': types.Float,
                'activeGeneration.measured': types.Float,
                'activeConsumption.measured': types.Float,
                'reactiveGeneration.measured': types.Float,
                'reactiveConsumption.measured': types.Float,
                'reactiveDemandOffPeak.measured': types.Float,
                'reactiveDemandPeak.measured': types.Float,
                'reactiveExcessConsumptionOffPeak.measured': types.Float,
                'reactiveExcessConsumptionPeak.measured': types.Float,
                'testActiveGeneration.measured': types.Float,
                'testReactiveGeneration.measured': types.Float,
                'higherHourlyConsumption.measured': types.Float,
                'consumptionPeak.adjusted': types.Float,
                'consumptionOffPeak.adjusted': types.Float,
                'demandPeak.adjusted': types.Float,
                'demandOffPeak.adjusted': types.Float,
                'demandSeasonalityPeak.adjusted': types.Float,
                'demandSeasonalityOffPeak.adjusted': types.Float,
                'activeConsumption.adjusted': types.Float,
                'reactiveExcessConsumptionOffPeak.adjusted': types.Float,
                'reactiveExcessConsumptionPeak.adjusted': types.Float,
                'activeGeneration.adjusted': types.Float,
                'reactiveGeneration.adjusted': types.Float,
                'reactiveConsumption.adjusted': types.Float,
                'reactiveDemandPeak.adjusted': types.Float,
                'testActiveGeneration.adjusted': types.Float,
                'testReactiveGeneration.adjusted': types.Float,
                'reactiveDemandOffPeak.adjusted': types.Float,
                'higherHourlyConsumption.adjusted': types.Float


            }

            columns_with_lists = ['reference']
            
            df = ins.convert_column_to_datetime(df, columns_with_lists)

            ins.insert_dataframe_from_table(df, database, schema, tab, 'append', forced_dtype)


    # PROCESSO PARALELO COM CHUCKSIZE DO DATFRAME E EXECUÇÃO DE CADA PARTE
    def execute_thread_pool_paralell(self, df, database, schema, tab):

        # Define o tamanho do chunk (exemplo: 5000 registros por inserção)
        chunksize = 5000

        # Divide o DataFrame em chunks
        chunks = [df[i:i+chunksize] for i in range(0, len(df), chunksize)]
        
        # função para execução de isntrução sql
        ins = InsertSqlServer()
        # ETAPA DELEÇÃO: DELETE FROM

        msg = f" | {tab} | Deletar dados existentes na tabela no SQL Server"
        print(datetime.datetime.now(),f"{msg.upper()}\n") 
        
        if ins.exists_from_table_all(schema, tab):
            ins.delete_from_table_all(df, database, schema, tab)
            
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures=[executor.submit(self.prepared_dataframe_from_table, chunckies, database, schema, tab) for   chunckies in chunks]
                
            for future in as_completed(futures):
                future.result()
                #time.sleep(60)
        
            executor.shutdown(wait=True)

                            
    # PROCESSO PARALELO COM QUBRA DE PROCESSO E EXECUÇÃO SIMULTANEA
    def execute_thread_pool_paralell_partial(self, df, database, schema, tab):
    
        # df_payments = pd.concat([df_income_to_sql, df_expenses_to_sql], ignore_index=True)

        lists_payments = list(map(list, df.itertuples(index=False)))

        # Lotes de dados
        batch_size = len(lists_payments)//5
        # arredonda para um numero inteiro de execuções em loop
        num_batches = (len(lists_payments) // batch_size) + 1

        for batch_num in range(num_batches):
            
            # apartir da execução atual multiplica pelo lote de dados(1*batch_size 2*batch_size)
            start_index = batch_num * batch_size
            #print(start_index)

            # ate a ultima partição desejada para particionar a execução, o minino dos valors inicial+lote
            # faz sentido na ultima execução que constroe a partição para execução do restante de dados                   
            end_index = min(start_index + batch_size, len(lists_payments))
            #print(end_index)
            if batch_num==0:
                # lote filtrado pelo tamanho do batch calculado           
                batch_payments_1 = lists_payments[start_index:end_index]
                
            elif batch_num==1:
                # lote filtrado pelo tamanho do batch calculado           
                batch_payments_2 = lists_payments[start_index:end_index]

            elif batch_num==2:
                # lote filtrado pelo tamanho do batch calculado           
                batch_payments_3 = lists_payments[start_index:end_index]

            elif batch_num==3:
                # lote filtrado pelo tamanho do batch calculado           
                batch_payments_4 = lists_payments[start_index:end_index]

            elif batch_num==4:
                # lote filtrado pelo tamanho do batch calculado           
                batch_payments_5 = lists_payments[start_index:end_index]

            elif batch_num==5:
                # lote filtrado pelo tamanho do batch calculado           
                batch_payments_6 = lists_payments[start_index:end_index]
                                                                                
            # print(f"Processing batch {batch_num + 1}/{num_batches} with {len(batch_payments)} requests.")

        # Paralelizar as requisições dentro do batch
        with ThreadPoolExecutor(max_workers=10) as executor:
            
            list_batch = [batch_payments_1, batch_payments_2, batch_payments_3, batch_payments_4, batch_payments_5, batch_payments_6]
            futures = [executor.submit(self.func_execute_prepared_schema_table, list_batch_pay, database, schema, tab) for list_batch_pay in list_batch]
            
            for future in as_completed(futures):
                future.result()
                time.sleep(10)

            # Pausar por 60 segundos entre os lotes
            # if batch_num < num_batches - 1:
            #     print("Waiting 10 seconds before the next batch...")
            #     time.sleep(10)

        # df_final.reset_index(drop=True, inplace=True)
        print("Data fetch and processing completed.")
        
    # PROCESSO PARALELO COM CHUCKSIZE DO DATFRAME E EXECUÇÃO DE CADA PARTE
    def execute_process_pool_paralell(self, df, database, schema, tab):

        # Define o tamanho do chunk (exemplo: 5000 registros por inserção)
        chunksize = 5000

        # Divide o DataFrame em chunks
        chunks = [df[i:i+chunksize] for i in range(0, len(df), chunksize)]

        # Paralelizar as requisições dentro do batch
        with ProcessPoolExecutor(max_workers=5) as executor:
            executor.map(self.prepared_sql_from_table, chunks, database, schema, tab)
            executor.shutdown(wait=True)          