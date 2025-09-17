from prepared_table.insert_sql_server import InsertSqlServer
from sqlalchemy import types
from conectores.conectar_sql_server import ConectSqlServer
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

import time

class PreparedSchemaTableFinanceThunders:
    
    def __init__(self):
        pass
        # self.database = 'TREINAMENTO'
        # self.schema = 'lkok'
            
    def execute_prepared_schema_table(self, df, database, tab):

        ins = InsertSqlServer()
                    
        if tab=='Incomes':

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
                        'invoiceLastStatusDate': types.String(length=255),
                        'presentationDate': types.String(length=255),
                        'dueDate': types.String(length=255),
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
                        'previsionDate': types.String(length=255),
                        'effectiveDate': types.String(length=255),
                        'paymentStatusId': types.String(length=255),
                        'paymentStatus': types.String(length=255),
                        'previsionValue':types.Float, 
                        'discountValue': types.Float, 
                        'penalityValue': types.Float, 
                        'delayedValue':  types.Float, 
                        'remainingValue':types.Float, 
                        'effectiveValue':types.Float, 
                        'discountedNetInstallment': types.Float,
                        'finalValue': types.Float, 
                        'orderItemTypeId': types.String(length=255),
                        'orderItemType': types.String(length=255),
                        'ordemItemClassifications': types.String(length=255),
                        'consolidationFirst': types.String(length=255),
                        'consolidationSecond': types.String(length=255),
                        'isCreatedByUser': types.Boolean,
                        'hasIntegrationError': types.Boolean,
                        'approvedDate': types.String(length=255),
                        'invoiceExtendedPropertyValues': types.String(length=255),
                        'companyExtendedPropertyValues': types.String(length=255)
                        }

            columns_with_lists = ['invoiceLastStatusDate', 'presentationDate', 'dueDate', 'previsionDate', 'effectiveDate', 'approvedDate']  # Replace with your column names
            df = ins.convert_column_to_datetime(df, columns_with_lists) 
                                                
            # Assuming df_final is your DataFrame
            ins.send_df_to_sql_with_schema(df, "APIpayments", 'Incomes', database, forced_dtype)
        
        elif tab=='Expenses':

            forced_dtype = {
                                'orderTradeTypeId': types.String(length=255),
                                'orderTradeType'  : types.String(length=255),
                                'orderId'         : types.String(length=255),
                                'orderCode'       : types.String(length=255),
                                'clientOrderNumber': types.String(length=255),
                                'invoiceId'       : types.String(length=255),
                                'invoiceCode'     : types.String(length=255),
                                'invoiceSequence' : types.String(length=255),
                                'orderCodeCcee'   : types.String(length=255),
                                'partyId'         : types.String(length=255),
                                'partyCnpj'       : types.String(length=255),
                                'partyName'       : types.String(length=255),
                                'partyAliasName'  : types.String(length=255),
                                'counterPartyId'  : types.String(length=255),
                                'counterPartyCpfCnpj': types.String(length=255),
                                'counterpartyName': types.String(length=255),
                                'counterPartyTypeId' : types.String(length=255),
                                'counterPartyAliasName' : types.String(length=255),
                                'counterPartyState': types.String(length=255),
                                'invoiceLastStatusDate': types.String(length=255),
                                'presentationDate': types.String(length=255),
                                'dueDate'         : types.String(length=255),
                                'invoiceStatusId' : types.String(length=255),
                                'invoiceStatusDescription': types.String(length=255),
                                'invoiceNumber'   : types.String(length=255),
                                'invoiceSerie'    : types.String(length=255),
                                'netValue':types.Float,
                                'totalValue':types.Float, 
                                'hasMultipleItens': types.Boolean,
                                'itemQuantity':types.Float, 
                                'itemTotalValue':types.Float, 
                                'itemUnitPrice':types.Float,
                                'itemUnitPriceWithIcms':types.Float, 
                                'invoiceIcmsValue':types.Float,
                                'invoiceDiscountValue':types.Float,
                                'invoiceItemIcmsAliquot':types.Float,
                                'invoiceCfop'     : types.String(length=255),
                                'itemDescription' : types.String(length=255),
                                'paymentID'       : types.String(length=255),
                                'previsionDate'   : types.String(length=255),
                                'effectiveDate'   : types.String(length=255),
                                'paymentStatusId' : types.String(length=255),
                                'paymentStatus'   : types.String(length=255),
                                'previsionValue':types.Float, 
                                'discountValue':types.Float,
                                'penalityValue':types.Float,
                                'delayedValue':types.Float,
                                'remainingValue':types.Float,
                                'effectiveValue':types.Float,
                                'discountedNetInstallment':types.Float,
                                'finalValue':types.Float,
                                'orderItemTypeId' : types.String(length=255),
                                'orderItemType'   : types.String(length=255),
                                'ordemItemClassifications': types.String(length=255),
                                'consolidationFirst': types.String(length=255),
                                'consolidationSecond': types.String(length=255),
                                'isCreatedByUser': types.Boolean,
                                'hasIntegrationError': types.Boolean,
                                'approvedDate'    : types.String(length=255),
                                'invoiceExtendedPropertyValues': types.String(length=255),
                                'companyExtendedPropertyValues': types.String(length=255)          
                        }

            columns_with_lists = ['invoiceLastStatusDate', 'presentationDate', 'dueDate', 'previsionDate', 'effectiveDate', 'approvedDate']  # Replace with your column names
            df = ins.convert_column_to_datetime(df, columns_with_lists) 
                                                
            # Assuming df_final is your DataFrame
            ins.send_df_to_sql_with_schema(df, "APIpayments", 'Expenses', database, forced_dtype)
        
        elif tab=='Payments':

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
                        'invoiceLastStatusDate': types.String(length=255),
                        'presentationDate': types.String(length=255),
                        'dueDate': types.String(length=255),
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
                        'previsionDate': types.String(length=255),
                        'effectiveDate': types.String(length=255),
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
                        'approvedDate': types.String(length=255),
                        'invoiceExtendedPropertyValues': types.String(length=255),
                        'companyExtendedPropertyValues': types.String(length=255)
                        }

            columns_with_lists = ['invoiceLastStatusDate', 'presentationDate', 'dueDate', 'previsionDate', 'effectiveDate', 'approvedDate']  # Replace with your column names
            df = ins.convert_column_to_datetime(df, columns_with_lists) 
                                               
            # Assuming df_final is your DataFrame
            ins.send_df_to_sql_with_schema(df, "APIpayments", 'Payments', database, forced_dtype)        
