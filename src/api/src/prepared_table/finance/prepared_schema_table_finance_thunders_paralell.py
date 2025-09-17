from prepared_table.insert_sql_server import InsertSqlServer
from prepared_table.finance.validation_columns import get_columns_sql

from sqlalchemy import types
from conectores.conectar_sql_server import ConectSqlServer
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

import pandas as pd
import datetime

class PreparedSchemaTableFinanceThundersParalell:
    
    def __init__(self):
        pass
        # self.database = 'TREINAMENTO'
        # self.schema = 'lkok'



    # INSERÇÃO VIA SCRIPT SQL DELETE FROM E DEPOIS INSERT INTO
    def prepared_sql_from_table(self, df, database, schema, tab):
        
        # função para execução de isntrução sql
        ins = InsertSqlServer()

        # ETAPA DELEÇÃO: DELETE FROM
        ins.delete_from_table_finance(df, database, schema, tab)
            
        columns_with_lists = ['invoiceLastStatusDate', 'presentationDate', 'dueDate', 'previsionDate', 'effectiveDate', 'approvedDate']  # Replace with your column names
        
        df = ins.convert_column_to_datetime(df, columns_with_lists) 
                                            
        ins.insert_sql_from_table(df, database, "APIpayments", 'Payments') #df, database, schema, tab)
                    
 
    # PROCESSO EM TESTES INSERÇÃO VIA DATAFRAME: APPEND OU REPLACE
    def prepared_dataframe_from_table(self, df, database, schema, tab):
        
        # função para execução de isntrução sql
        ins = InsertSqlServer()
        
        #Substituir Valores NaN por None
        df = df.where(pd.notnull(df), None)
           
        forced_dtype = {
                    'orderTradeTypeId': types.String(length=255),
                    'orderTradeType': types.String(length=255),
                    'orderId': types.String(length=255),
                    'orderCode': types.String(length=255),
                    'clientOrderNumber': types.String(length=255),
                    'invoiceId': types.String(length=255),
                    'invoiceCode': types.String(length=255),
                    'invoiceSequence': types.String(length=255),
                    'orderCodeCcee': types.String(length=255),
                    'partyId': types.String(length=255),
                    'partyCnpj': types.String(length=255),
                    'partyName': types.String(length=255),
                    'partyAliasName': types.String(length=255),
                    'counterPartyId': types.String(length=255),
                    'counterPartyCpfCnpj': types.String(length=255),
                    'counterpartyName': types.String(length=255),
                    'counterPartyTypeId': types.String(length=255),
                    'counterPartyAliasName': types.String(length=255),
                    'counterPartyState': types.String(length=255),
                    'invoiceLastStatusDate': types.DateTime,
                    'presentationDate': types.DateTime,
                    'dueDate': types.DateTime,
                    'invoiceStatusId': types.String(length=255),
                    'invoiceStatusDescription': types.String(length=255),
                    'invoiceNumber': types.String(length=255),
                    'invoiceSerie': types.String(length=255),
                    'netValue': types.Float,
                    'totalValue': types.Float, 
                    'hasMultipleItens': types.Boolean,
                    'itemQuantity': types.Float, 
                    'itemTotalValue': types.Float,
                    'itemUnitPrice': types.Float,
                    'itemUnitPriceWithIcms': types.Float,
                    'invoiceIcmsValue': types.Float, 
                    'invoiceDiscountValue': types.Float, 
                    'invoiceItemIcmsAliquot': types.Float, 
                    'invoiceCfop': types.String(length=255),
                    'itemDescription': types.String(length=255),
                    'paymentID': types.String(length=255),
                    'previsionDate': types.DateTime,
                    'effectiveDate': types.DateTime,
                    'paymentStatusId': types.String(length=255),
                    'paymentStatus': types.String(length=255),
                    'previsionValue': types.Float, 
                    'discountValue': types.Float, 
                    'penalityValue': types.Float, 
                    'delayedValue': types.Float, 
                    'remainingValue': types.Float, 
                    'effectiveValue': types.Float, 
                    'discountedNetInstallment': types.Float, 
                    'finalValue': types.Float, 
                    'orderItemTypeId': types.String(length=255),
                    'orderItemType': types.String(length=255),
                    'ordemItemClassifications': types.String(length=255),
                    'consolidationFirst': types.String(length=255),
                    'consolidationSecond': types.String(length=255),
                    'isCreatedByUser': types.Boolean,
                    'hasIntegrationError': types.Boolean,
                    'approvedDate': types.DateTime,
                    'invoiceExtendedPropertyValues': types.String(length=255),
                    'companyExtendedPropertyValues': types.String(length=255),
                    'reimbursementValue': types.String(length=255),
                    'integrationStatusId': types.String(length=255)                   
                    }
        
        # Convert list columns to JSON strings
        columns_with_lists = ['invoiceLastStatusDate', 'presentationDate', 'dueDate', 'previsionDate', 'effectiveDate', 'approvedDate']  # Replace with your column names
        
        df = ins.convert_column_to_datetime(df, columns_with_lists) 

        # ETAPA INSERÇÃO: INSERT INTO
        ins.insert_dataframe_from_table(df, database, "APIpayments", 'Payments', 'append', forced_dtype)

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
        
        # Deleta os dados existentes na tabela
        ins.delete_from_table_all(df, database, schema, tab)
            
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures=[executor.submit(self.prepared_dataframe_from_table, chunckies, database, schema, tab) for   chunckies in chunks]
                
            for future in as_completed(futures):
                future.result()
                #time.sleep(60)
        
            executor.shutdown(wait=True)
  
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