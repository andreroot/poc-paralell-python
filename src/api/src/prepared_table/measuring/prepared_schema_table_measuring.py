from prepared_table.insert_sql_server import InsertSqlServer
from sqlalchemy import types

class PreparedSchemaTableMeasuring:
    
    def __init__(self):
        self.database = 'TREINAMENTO'
        self.schema = 'lkok'
            
    def execute_prepared_schema_table(self, df, tab):

        ins = InsertSqlServer()

        if tab=='MeasuringPoint':
            # df_api_MeasuringProjectionConsolidateMonthYear
            forced_dtype = {
                'id': types.String(length=100),
                'startDate': types.DateTime,
                'endDate': types.DateTime,
                'typeId': types.Integer,
                'name': types.String(length=255),
                'code': types.String(length=255),
                'type': types.String(length=50),
                'assets': types.String(length=255),
                'collectionMode': types.String(length=50),
                'consumerUnity': types.String(length=100)
            }

            columns_with_lists = ['startDate', 'endDate']
            df = ins.convert_column_to_datetime(df, columns_with_lists)

        elif tab=='MeasuringPoint_details':

            # df_api_MeasuringProjectionConsolidateMonthYear
            forced_dtype = {
                'id': types.String(length=100),
                'name': types.String(length=255),
                'code': types.String(length=255),
                'type': types.String(length=50),
                'assets': types.String(length=255),
                'collectionMode': types.String(length=50),
                'consumerUnity': types.String(length=100),
                'period.startDate.year'   : types.Integer,
                'period.startDate.month' :  types.Integer,
                'period.startDate.day'  :   types.Integer,
                'period.endDate.year'  :    types.Integer,
                'period.endDate.month':     types.Integer,
                'period.endDate.day':       types.Integer,
                'consumptionProjection.months':       types.Integer,
                'demandProjection.months':       types.Integer,
                'ownerProfileId':       types.String(length=100)
            }
            #convert_column_to_datetime(df_MeasuringPoint_details, 'startDate')
            # Columns to remove
            columns_to_remove = ['testPeriods', 'associatedNetworks',	'associatedAssets']

            # Convert list columns to JSON strings
            columns_with_lists = ['testPeriods', 'associatedNetworks',	'associatedAssets']  # Replace with your column names
            df = ins.convert_lists_to_text(df, columns_with_lists)

            # ins.convert_column_to_datetime(df_MeasuringPoints, 'endDate')
                    
        elif tab=='MeasuringPoint_associatedAssets':

            # df_api_MeasuringProjectionConsolidateMonthYear
            forced_dtype = {
                'id': types.String(length=255),
                'isMeasurementProfileSameAsAccountingProfile': types.String(length=255),
                'profiles': types.String(length=255),
                'parcels': types.String(length=255),
                'accountingProfiles': types.String(length=255),
                'parent_id': types.String(length=100),
                'parent_code': types.String(length=255),
                'parent_name': types.String(length=255),
                'period.startDate.year': types.Integer,
                'period.startDate.month': types.Integer,
                'period.startDate.day': types.Integer,
                'physicalAsset.id': types.String(length=255),
                'physicalAsset.name': types.String(length=255),
                'physicalAsset.companyId': types.String(length=255),
                'physicalAsset.companyName': types.String(length=255),
                'period.endDate.year': types.Integer,
                'period.endDate.month': types.Integer,
                'period.endDate.day': types.Integer,
            }

            # Convert list columns to JSON strings
            columns_with_lists = ['profiles', 'parcels',	'accountingProfiles']  # Replace with your column names
            
            df = ins.convert_lists_to_text(df, columns_with_lists)
                                    
        # Assuming df_final is your DataFrame
        ins.send_df_to_sql_with_schema(df, self.schema, tab, self.database, forced_dtype)
        
        