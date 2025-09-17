from prepared_table.insert_sql_server import InsertSqlServer
from sqlalchemy import types

class PreparedSchemaTableFinance:
    
    def __init__(self):
        self.database = 'TREINAMENTO'
        self.schema = 'lkok'
            
    def execute_prepared_schema_table(self, df, tab):

        ins = InsertSqlServer()

        if tab=='paymentconditions_data':
            forced_dtype = {
                        'id': types.String(length=255),
                        'companyId': types.String(length=255),
                        'company': types.String(length=255),
                        'description': types.String(length=255),
                        'incidenceType': types.String(length=255),
                        'active': types.Boolean,
                        'default': types.Boolean,
                        }

            # Assuming df_final is your DataFrame
            ins.insert_dataframe_from_table(df, self.database, self.schema, tab, 'replace',  forced_dtype)
        
        elif tab=='InvoiceExpirationType':

            forced_dtype = {
                'id': types.Integer,
                'name': types.String(length=255),
                'isCustom': types.Boolean,
                'isDaysRequired': types.Boolean,
            }            

            # Assuming df_final is your DataFrame
            ins.insert_dataframe_from_table(df, self.database, self.schema, tab, 'replace',  forced_dtype)
            
        elif tab=='demands':
            # df_loadPhysicalAssets_details
            forced_dtype = {
                        'id': types.String(length=255),
                        'companyId': types.String(length=255),
                        'company': types.String(length=255),
                        'description': types.String(length=255),
                        'incidenceType': types.String(length=255),
                        'active': types.Boolean,
                        'default': types.Boolean,
                        }

            columns_with_lists = ['demands', 'industrializationPercentagePeriods', 'parcels', 'generator.periods']  # Replace with your column names
            df = ins.convert_lists_to_text(df, columns_with_lists)            

            # Assuming df_final is your DataFrame
            ins.insert_dataframe_from_table(df, self.database, self.schema, tab, 'replace',  forced_dtype)

# demands	
# economySinceAcl	
# generator	
# useFiveMinutesReading	
# industrializationPercentagePeriods	
# id	
# name	
# hasSameCompanyAddress	
# connection	
# parcels	
# covidTariffDateRequest	
# company.id	
# company.name	
# company.alias	
# company.isDeleted	
# address.street	
# address.number	
# address.complement	
# address.neighborhood	
# address.city	
# address.state	
# address.zipcode	
# connectionType.id	
# connectionType.name	
# connection.id	
# connection.name
# connection.initials

        elif tab=='loadPhysicalAssets_details_v1':

# id	
# startDate	
# endDate	
# contractedValue	
# peak	
# offPeak	
# acrSameAcl	
# peakAcr	
# offPeakAcr	
# useEnergyGenerator	
# costEnergyGenerator	
# capacityEnergyGenerator	
# hasTest	
# endDateTest	
# peakReasonTestId	
# offPeakReasonTestId	
# peakReasonTest	
# offPeakReasonTest	
# parent_id	
# parent_name	
# subGroup.id	
# subGroup.description	
# modality.id	
# modality.description	
# acrModality.id	
# acrModality.description

            # df_api_MeasuringProjectionConsolidateMonthYear
            forced_dtype = {
            'demands': types.Text,
                'economySinceAcl': types.String(length=255),
                'useFiveMinutesReading': types.Boolean,
                'industrializationPercentagePeriods': types.String(length=255),
                'id': types.String(length=255),
                'name': types.String(length=255),
                'hasSameCompanyAddress': types.Boolean,
                'connection': types.String(length=255),
                'parcels': types.String(length=255),
                'covidTariffDateRequest': types.String(length=255),
                'generator.enabled': types.Boolean,
                'generator.periods': types.String(length=255),
                'company.id': types.String(length=255),
                'company.name': types.String(length=255),
                'company.alias': types.String(length=255),
                'company.isDeleted': types.Boolean,
                'address.street': types.String(length=255),
                'address.number': types.String(length=255),
                'address.complement': types.String(length=255),
                'address.neighborhood': types.String(length=255),
                'address.city': types.String(length=255),
                'address.state': types.String(length=255),
                'address.zipcode': types.String(length=255),
                'connectionType.id': types.Integer,
                'connectionType.name': types.String(length=255),
                'connection.id': types.String(length=255),
                'connection.name': types.String(length=255),
                'connection.initials': types.String(length=255)
            }
            #convert_column_to_datetime(loadPhysicalAssets_details, 'startDate')
            #convert_column_to_datetime(loadPhysicalAssets_details, 'endDate')

            # Columns to remove
            #columns_to_remove = ['testPeriods', 'associatedNetworks',	'associatedAssets']

            # Convert list columns to JSON strings
            columns_with_lists = ['demands', 'industrializationPercentagePeriods', 'parcels', 'generator.periods']  # Replace with your column names
            # df_cleaned = convert_lists_to_text(df_loadPhysicalAssets_details, columns_with_lists)

            # # Remove columns
            # #df_cleaned = remove_columns(df_MeasuringPoint_details, columns_to_remove)
            # # Assuming df_final is your DataFrame
            # send_df_to_sql_with_schema(df_cleaned, 'lkok', 'loadPhysicalAssets_details', server, database, forced_dtype)

            df = ins.convert_lists_to_text(df, columns_with_lists)            

            # Assuming df_final is your DataFrame
            ins.insert_dataframe_from_table(df, self.database, self.schema, tab, 'replace',  forced_dtype)
