from prepared_table.insert_sql_server import InsertSqlServer
from sqlalchemy import types

class PreparedSchemaTableGuaranted:
    
    def __init__(self):
        self.database = 'TREINAMENTO'
        self.schema = 'lkok'
            
    def execute_prepared_schema_table(self, df, tab):

        ins = InsertSqlServer()

        if tab=='guaranteed_savings_charges':
            
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

        elif tab=='MonthView':

            # df_api_MeasuringProjectionConsolidateMonthYear
            forced_dtype = {
                'flexibilityJson': types.String(length=255),
                'sequence': types.Float,
                'periodReference': types.String(length=255),
                'id': types.String(length=255),
                'code': types.String(length=255),
                'partyId': types.String(length=255),
                'party': types.String(length=255),
                'cnpjParty': types.String(length=255),
                'counterPartyId': types.String(length=255),
                'counterParty': types.String(length=255),
                'cnpjCounterParty': types.String(length=255),
                'startPeriod': types.DateTime,
                'endPeriod':types.DateTime,
                'classifications': types.String(length=255),
                'portfolios': types.String(length=255),
                'operator': types.String(length=255),
                'isActive': types.String(length=255),
                'managementDealId': types.String(length=255),
                'hasRemuneration': types.String(length=255),
                'businessUnit': types.Float,
                'physicalAssetsCount': types.Float,
                'totalCostACR': types.Float,
                'totalCostACL': types.Float,
                'volume': types.Float,
                'minorValue': types.Float,
                'averageValue': types.Float,
                'maxValue': types.Float,
                'savingsPercentageContracted': types.Float,
                'savingsPercentageAccomplished': types.Float,
                'calculateFormulaId': types.String(length=255),
                'calculateFormulaName': types.String(length=255),
                'invoicingStatus': types.String(length=255),
                'savingsBalancePastMonth': types.Float,
                'savingsBalanceCurrent': types.Float,
                'savingsBalanceUsed': types.Float,
                'savingsBalanceGenerated': types.Float,
                'managementDealBillingStatus': types.Float,
                'managementDealBillingPartialApproval': types.String(length=255),
                'createdDate': types.String(length=255),
                'observation': types.String(length=255),
                'contractPeriodStart': types.String(length=255),
                'contractPeriodEnd': types.String(length=255),
                'volumeExceeded': types.Float,
                'hasCharges': types.String(length=255),
                'managementDealBillingStatusName': types.String(length=255),
                'hasZeroValue': types.String(length=255),
                'lastUpdate': types.String(length=255),
                'flexibility': types.String(length=255),
                'flexUsagePercentage': types.String(length=255),
                'year': types.Integer,
                'month': types.Integer,
            }

            columns_with_lists = ['startPeriod', 'endPeriod']
            df = ins.convert_column_to_datetime(df, columns_with_lists)
           
        elif tab=='ContractManagementPeriod':

            # df_api_MeasuringProjectionConsolidateMonthYear
            forced_dtype = {
                'reference': types.DateTime,
                'contractId': types.String(length=255),
                'physicalAssetCount': types.Integer,
                'billingStatusId': types.Integer,
                'billingStatus': types.String(length=255),
                'formula': types.String(length=255),
                'aclCost': types.Float,
                'acrCost': types.Float,
                'forecastPrice': types.Float,
                'adjustmentPrice': types.Float,
                'approvedPrice': types.Float,
                'postBillingPrice': types.Float,
                'forecastVolume': types.Float,
                'adjustmentVolume': types.Float,
                'approvedVolume': types.Float,
                'postBillingVolume': types.Float,
                'forecastSavingsPercentage': types.Float,
                'adjustmentSavingsPercentage': types.Float,
                'approvedSavingsPercentage': types.Float,
                'postBillingSavingsPercentage': types.Float,
                'previousBalance': types.Float,
                'usedBalance': types.Float,
                'generatedBalance': types.Float,
                'monthlyBalance': types.Float,
                'forecastMtm': types.Float,
                'adjustmentMtm': types.Float,
                'approvedMtm': types.Float,
                'postBillingMtm': types.Float,
                'feesValue': types.Float,
                'feesBillingStatusId': types.Integer,
                'feesBillingStatus': types.String(length=255),
                'hasZeroValue': types.Boolean,
                'balanceExcessBilling': types.Integer,
                'balanceExcessPostBilling': types.Float,
                'balanceExcessConsumption': types.Float,
                'lastBalanceExcessConsumption': types.Float,
            }

            columns_with_lists = ['reference']
            df = ins.convert_column_to_datetime(df, columns_with_lists)
        
        elif tab=='CalculationFormula':

            forced_dtype = {
                'id': types.String(length=50),
                'name': types.String(length=255),
                'description': types.String(length=255),
                'createdDate': types.DateTime,
                'updatedDate': types.DateTime,
                'canEdit': types.Boolean,
            }  
            

            columns_with_lists = ['createdDate','updatedDate']
            df = ins.convert_column_to_datetime(df, columns_with_lists)
                                                             
        # Assuming df_final is your DataFrame
        ins.insert_dataframe_from_table(df, self.database, self.schema, tab, 'replace',  forced_dtype)
        
        