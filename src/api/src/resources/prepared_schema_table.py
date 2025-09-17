from resources.insert_sql_server import InsertSqlServer
from sqlalchemy import types

class PreparedSchemaTable:
    
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
                
        elif tab=='MeasuringProjectionConsolidateMonthYear':
            forced_dtype = {
                'period': types.DateTime,
                'hoursInPeriod': types.Integer,
                'id': types.String(length=100),
                'consumptionPeak.projected': types.Float,
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

        elif tab=='MeasuringAdjustConsolidated':
            forced_dtype = {
                'id': types.String(length=100),
                'reference': types.DateTime
            }
            
            columns_with_lists = ['reference']
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

        elif tab=='MeasuringAdjust':
                
            # df_api_MeasuringProjectionConsolidateMonthYear
            forced_dtype = {
                'measuringPointId': types.String(length=255),
                'physicalAssetId': types.String(length=255),
                'isCompleted': types.Boolean,
                'isZero': types.Boolean,
                'period': types.DateTime,
                'activeGeneration': types.Float,
                'activeConsumption': types.Float,
                'reactiveGeneration': types.Float,
                'reactiveConsumption': types.Float,
                }
            # Convert list columns to JSON strings
            columns_with_lists = ['period']
            
            df = ins.convert_column_to_datetime(df, columns_with_lists)

        elif tab=='guaranteed_savings_charges':
            
            # df_api_MeasuringProjectionConsolidateMonthYear
            forced_dtype = {
                'id': types.String(length=255),
                'partyId': types.String(length=255),
                'counterPartyId': types.String(length=255),
                'operatorId': types.String(length=255),
                'code': types.String(length=255),
                'observation': types.String(length=255),
                'version': types.Integer,
                'businessUnit': types.Integer,
                'classifications': types.String(length=255),
                'portfolios': types.String(length=255),
                'servicePeriods': types.Text(),
                'managementDealSettings.userOperatorId': types.String(length=255),
                'managementDealSettings.marketingOperatorId': types.String(length=255),
                'managementDealSettings.classificationsIds': types.String(length=255),
                'managementDealSettings.partyId': types.String(length=255),
            }
            #convert_column_to_datetime(df_guaranteed_savings_charges_services, 'start')
            #convert_column_to_datetime(df_guaranteed_savings_charges_services, 'end')

            # Columns to remove
            #columns_to_remove = ['testPeriods', 'associatedNetworks',	'associatedAssets']

            # Convert list columns to JSON strings
            columns_with_lists = ['classifications', 'portfolios', 'servicePeriods', 'managementDealSettings.classificationsIds']  # Replace with your column names
            df = ins.convert_lists_to_text(df, columns_with_lists)

            # Remove columns
            #df_cleaned = remove_columns(df_MeasuringPoint_details, columns_to_remove)
            # Assuming df_final is your DataFrame
            #send_df_to_sql_with_schema(df_cleaned, 'lkok', 'guaranteed_savings_charges', server, database, forced_dtype)        

        elif tab=='guaranteed_savings_charges_services':

            # df_api_MeasuringProjectionConsolidateMonthYear
            forced_dtype = {
                'guaranteedSavingsContractId': types.String(length=255),
                'start': types.DateTime,
                'end': types.DateTime,
                'sequence': types.Integer,
                'savingsPercentage': types.Integer,
                'calculationFormula': types.Text,
                'calculationFormulaValue': types.Text,
                'paymentConditionId': types.String(length=255),
                'adjustmentPatternId': types.Integer,
                'hasApprovedBilling': types.Boolean,
                'hasApprovedBillingCharge': types.Boolean,
                'hasGenerateSavingsReport': types.Boolean,
                'flexibility': types.String(length=255),
                'excessConsumption': types.Float,
                'readjustments': types.Text,
                'physicalAssets': types.Text,
                'chargeConfiguration': types.Float,
                'minimumVolumeSettings': types.String(length=255),
                'guarantee': types.String(length=255),
                'parent_id': types.String(length=255),
                'parent_code': types.String(length=255),
                'remuneration.paymentConditionId': types.String(length=255),
                'remuneration.fixedValueByCceeAgent.invoiceTypeId': types.Integer,
                'remuneration.fixedValueByCceeAgent.apurationTypeId': types.Integer,
                'remuneration.fixedValueByCceeAgent.price': types.Integer,
                'remuneration.fixedValueByCceeAgent.isEnabled': types.Boolean,
                'remuneration.fixedValueByPhysicalAsset.apurationType': types.Integer,
                'remuneration.fixedValueByPhysicalAsset.invoiceType': types.Integer,
                'remuneration.fixedValueByPhysicalAsset.price': types.Integer,
                'remuneration.fixedValueByPhysicalAsset.isEnabled': types.Boolean,
                'remuneration.fixedValueByPhysicalAsset.physicalAssetValues': types.String(length=255),
                'automaticAdjustment.includeLoss': types.Boolean,
                'automaticAdjustment.includeProinfa': types.Boolean,
                'automaticAdjustment.lossId': types.String(length=255),
                'automaticAdjustment.transmissionLoss': types.Float,
                'automaticAdjustment.months': types.String(length=255),
                'chargeConfiguration.enabled': types.String(length=255),
                'chargeConfiguration.paymentChargeConditionTypeId': types.Float,
                'chargeConfiguration.paymentConfiguration': types.Float,
                'chargeConfiguration.charges': types.Text,
                'excessConsumption.includeReadjustInCalculation': types.Float,
                'excessConsumption.isDifferentiatedByPeriod': types.String(length=255),
                'excessConsumption.periods': types.String(length=255),
            }

            # Columns to remove
            #columns_to_remove = ['testPeriods', 'associatedNetworks',	'associatedAssets']
            columns_with_lists = ['start', 'end']
            df = ins.convert_column_to_datetime(df, columns_with_lists)
            
            # Convert list columns to JSON strings
            columns_with_lists = ['readjustments', 'physicalAssets', 'remuneration.fixedValueByPhysicalAsset.physicalAssetValues', 'chargeConfiguration.charges', 'excessConsumption.periods']  # Replace with your column names
            df = ins.convert_lists_to_text(df, columns_with_lists)

            # # Remove columns
            # #df_cleaned = remove_columns(df_MeasuringPoint_details, columns_to_remove)
            # # Assuming df_final is your DataFrame
            # send_df_to_sql_with_schema(df_cleaned, 'lkok', 'guaranteed_savings_charges_services', server, database, forced_dtype)

        elif tab=='guaranteed_savings_charges_services_chargeConfigurationcharges':
            
            # df_api_MeasuringProjectionConsolidateMonthYear
            forced_dtype = {
                'id': types.String(length=255),
                'startDate': types.String(length=255),
                'endDate': types.String(length=255),
                'contractedValue': types.Float,
                'peak': types.Float,
                'offPeak': types.Float,
                'acrSameAcl': types.Boolean,
                'peakAcr': types.Float,
                'offPeakAcr': types.Float,
                'useEnergyGenerator': types.Boolean,
                'costEnergyGenerator': types.String(length=255),
                'capacityEnergyGenerator': types.String(length=255),
                'hasTest': types.Boolean,
                'endDateTest': types.String(length=255),
                'peakReasonTestId': types.String(length=255),
                'offPeakReasonTestId': types.String(length=255),
                'peakReasonTest': types.String(length=255),
                'offPeakReasonTest': types.String(length=255),
                'parent_id': types.String(length=255),
                'parent_name': types.String(length=255),
                'parent_guaranteedSavingsContractId': types.String(length=255),
                'parent_start': types.String(length=255),
                'parent_end': types.String(length=255),
                'parent_sequence': types.Integer,
                'parent_code': types.String(length=255),
                'subGroup.id': types.Integer,
                'subGroup.description': types.String(length=255),
                'modality.id': types.Integer,
                'modality.description': types.String(length=255),
                'acrModality.id': types.Integer,
                'acrModality.description': types.String(length=255),
            }
            
            columns_with_lists = ['startDate', 'endDate', 'parent_start',  'parent_end']
            df = ins.convert_column_to_datetime(df, columns_with_lists)

        elif tab=='guaranteed_savings_charges_services_physicalAssets':

            # df_api_MeasuringProjectionConsolidateMonthYear
            forced_dtype = {
                'guaranteedSavingsContractServicePeriodId': types.String(length=255),
                'physicalAssetId': types.String(length=255),
                'subMarket': types.String(length=255),
                'energySource': types.String(length=255),
                'profileParty': types.String(length=255),
                'profileCounterParty': types.String(length=255),
                'accumulatedBalance': types.Float,
                'savingsPercentage': types.Float,
                'parent_id': types.Integer,
                'parent_code': types.String(length=255),
                'parent_guaranteedSavingsContractId': types.String(length=255),
                'parent_start': types.String(length=255),
                'parent_end': types.String(length=255),
                'parent_sequence': types.Integer,
            }
            
            columns_with_lists = ['parent_start', 'parent_end']
            df = ins.convert_column_to_datetime(df, columns_with_lists)
                                    
        # Assuming df_final is your DataFrame
        ins.send_df_to_sql_with_schema(df, self.schema, tab, self.database, forced_dtype)
        
        