USE [Modelo]
GO
/****** Object:  StoredProcedure [dbo].[STP_operation_history]    Script Date: 05/03/2025 10:49:47 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




ALTER PROCEDURE [dbo].[STP_operation_history] AS
BEGIN
	DROP TABLE IF EXISTS Modelo.dbo.proc_operation_history;
	CREATE TABLE Modelo.[dbo].[proc_operation_history](
		[code] [varchar](250) NULL,
		[sequence] [varchar](250) NULL,
		[operationType] [varchar](250) NULL,
		[tradeType] [varchar](250) NULL,
		[version] [varchar](250) NULL,
		[primaryOperationId] [varchar](250) NULL,
		[primaryOperationCode] [varchar](250) NULL,
		[primaryOperationSequence] [varchar](250) NULL,
		[isTrading] [bit] NULL,
		[isServices] [bit] NULL,
		[isGeneration] [bit] NULL,
		[businessUnitDescription] [varchar](250) NULL,
		[isActive] [bit] NULL,
		[partyId] [varchar](250) NULL,
		[partyCNPJ] [varchar](250) NULL,
		[partyName] [varchar](250) NULL,
		[partyAlias] [varchar](250) NULL,
		[partyAgentAcronym] [varchar](250) NULL,
		[partyAgentCode] [varchar](250) NULL,
		[partyProfileCode] [varchar](250) NULL,
		[partyProfileDescription] [varchar](250) NULL,
		[counterpartyId] [varchar](250) NULL,
		[counterpartyCNPJ] [varchar](250) NULL,
		[counterpartyName] [varchar](250) NULL,
		[counterpartyAlias] [varchar](250) NULL,
		[counterpartyAgentAcronym] [varchar](250) NULL,
		[counterpartyAgentCode] [varchar](250) NULL,
		[counterpartyProfileCode] [varchar](250) NULL,
		[counterpartyProfileDescription] [varchar](250) NULL,
		[counterPartyIsGroupCompany] [bit] NULL,
		[userOperatorName] [varchar](250) NULL,
		[submarketDescription] [varchar](250) NULL,
		[energySourceDescription] [varchar](250) NULL,
		[priceTypeDescription] [varchar](250) NULL,
		[startDate] [varchar](250) NULL,
		[endDate] [varchar](250) NULL,
		[contractedVolumeMwm] [decimal](20, 8) NULL,
		[contractedVolumeMwh] [decimal](20, 8) NULL,
		[seasonalityVolumeMwh] [decimal](20, 8) NULL,
		[seasonalityVolumeMwm] [decimal](20, 8) NULL,
		[finalVolumeMwh] [decimal](20, 8) NULL,
		[finalVolumeMwm] [decimal](20, 8) NULL,
		[basePrice] [decimal](20, 8) NULL,
		[price] [decimal](20, 8) NULL,
		[nominalPrice] [decimal](20, 8) NULL,
		[mtm] [decimal](20, 8) NULL,
		[retusd] [decimal](20, 8) NULL,
		[classifications] [varchar](250) NULL,
		[userCreatedName] [varchar](250) NULL,
		[createdAt] [varchar](250) NULL,
		[userModifiedName] [varchar](250) NULL,
		[modifiedAt] [varchar](250) NULL,
		[userDeletedName] [varchar](250) NULL,
		[deletedAt] [varchar](250) NULL,
		[userBackofficeName] [varchar](250) NULL,
		[userCommercialName] [varchar](250) NULL,
		[origin] [varchar](250) NULL,
		[bbceCode] [varchar](250) NULL,
		[hasFlexibility] [bit] NULL,
		[isFlexibilityLoadCurve] [bit] NULL,
		[isFlexibilityByPeriod] [bit] NULL,
		[flexibilityPercentageBottom] [float] NULL,
		[flexibilityPercentageTop] [float] NULL,
		[hasSeasonality] [bit] NULL,
		[isSeasonalityByPeriod] [bit] NULL,
		[seasonalityPercentageBottom] [float] NULL,
		[seasonalityPercentageTop] [float] NULL,
		[hasModulation] [bit] NULL,
		[isModulationLoadCurve] [bit] NULL,
		[hasDefaultFinancialFlow] [bit] NULL,
		[hasReadjustment] [bit] NULL,
		[reajustmentIndex] [varchar](250) NULL,
		[readjustmentBaseDate] [varchar](250) NULL,
		[readjustmentFirstDate] [varchar](250) NULL,
		[hasGuarantee] [bit] NULL,
		[guaranteeValue] [decimal](20, 8) NULL,
		[guaranteeDueDate] [varchar](250) NULL,
		[guaranteeTypes] [varchar](250) NULL,
		[hasRepresentativeFactor] [bit] NULL,
		[representativeFactorPercent] [decimal](20, 8) NULL,
		[losses] [decimal](20, 8) NULL,
		[cceeContractCode] [varchar](250) NULL,
		[needApportionment] [varchar](250) NULL,
		[spread] [decimal](20, 8) NULL,
		[floor] [decimal](20, 8) NULL,
		[ceiling] [decimal](20, 8) NULL,
		[billingStatus] [varchar](250) NULL,
		[aprovalStatusId] [int] NULL,
		[aprovalStatusDescription] [varchar](250) NULL,
		[year] [int] NULL,
		[month] [int] NULL,
		[startDay] [int] NULL,
		[endDay] [int] NULL,
		[id] [varchar](250) NULL,
		[operationTypeId] [int] NULL,
		[_Link] [varchar](250) NULL,
		[syncCreatedAt] [varchar](250) NULL,
		[syncDeletedAt] [varchar](250) NULL,
		[syncUpdatedAt] [varchar](250) NULL,
		[priceVariableTypeId] [int] NULL,
		[priceVariableIndex] [varchar](250) NULL,
		[isDraft] [bit] NULL,
		[userOperatorCouterPartyName] [varchar](250) NULL,
		[portfolios] [varchar](250) NULL,
		[consentingIntervenerCompanyName] [varchar](250) NULL,
		[proposalCode] [varchar](250) NULL,
		[proposalId] [varchar](250) NULL,
		[basePriceWithReadjustment] [decimal](20, 8) NULL,
		[parentOperationId] [varchar](255) NULL,
		[availableToUpdateApportionment] [bit] NULL,
		[isDistribuition] [bit] NULL,
		[counterpartyTypeId] [int] NULL,
		[partyTypeId] [int] NULL,
		[codeCcee] [varchar](250) NULL,
		[draftCreatedDate] [varchar](255) NULL,
		[observations] [varchar](max) NULL,
		[counterpartyIsAgentInMigration] [bit] NULL,
		[partyIsAgentInMigration] [bit] NULL,
		[isRetail] [bit] NULL,
		[modulationFormTypeId] [int] NULL,
		[excessConsumptionChargePrice] [decimal](20, 8) NULL,
		[hasExcessConsumptionCharge] [bit] NULL,
		[surplusVolumeMwh] [decimal](20, 8) NULL,
		[surplusVolumeMwm] [decimal](20, 8) NULL,
		[totalExcessConsumptionCharge] [decimal](20, 8) NULL,
		[totalVolumeMWh] [decimal](20, 8) NULL,
		[codeIntegration] [varchar](250) NULL,
		[n5xCode] [varchar](250) NULL,
		[hasBalanceManagement] [bit] NULL,
		[balanceValue] [varchar](255) NULL,
		[balanceVolumeMwh] [decimal](20, 8) NULL,
		[flexUsagePercentage] [decimal](20, 8) NULL,
		[consentingIntervenerPartyName] [varchar](250) NULL,
		[totalVolumeMWm] [decimal](20, 8) NULL,
		[isServicesOriginal] [bit] NULL,
		[ValidFrom] DATETIME2 NOT NULL,
		[ValidTo] DATETIME2 NOT NULL,
		[IsSyncDeletedAtNull] BIT NOT NULL,
		[Thunders] VARCHAR(20) NOT NULL,
	);
	with base as (
		SELECT *,
			[validFrom] = COALESCE(
				CAST(case
					when [syncCreatedAt] IS NOT NULL then [syncCreatedAt]
					when [syncDeletedAt] IS NOT NULL then [syncDeletedAt]
					else [syncUpdatedAt]
				end AS DATETIME2),
				CAST('0001-01-01T00:00:00.000Z' AS DATETIME2)
		
			),
			[validTo] = 
				lead(
					CAST(case when [syncCreatedAt] IS NOT NULL then [syncCreatedAt] when [syncDeletedAt] IS NOT NULL then [syncDeletedAt] else [syncUpdatedAt] end AS DATETIME2)
					, 1
					, CAST('9999-12-31T23:59:59.9999999Z' AS DATETIME2)
				) OVER (
					PARTITION BY id,
						year,
						month,
						sequence,
						businessUnitDescription
					ORDER BY (
						case
							when [syncCreatedAt] IS NOT NULL then [syncCreatedAt]
							when [syncDeletedAt] IS NOT NULL then [syncDeletedAt]
							else [syncUpdatedAt] end)
					),
			IsSyncDeletedAtNull = CASE WHEN syncDeletedAt is null then 1 else 0 end,
			Thunders = 'Safira'
		FROM Book.dbo.operation_history

		UNION ALL

		SELECT *,
			[validFrom] = COALESCE(
				CAST(case
					when [syncCreatedAt] IS NOT NULL then [syncCreatedAt]
					when [syncDeletedAt] IS NOT NULL then [syncDeletedAt]
					else [syncUpdatedAt]
				end AS DATETIME2),
				CAST('0001-01-01T00:00:00.000Z' AS DATETIME2)
		
			),
			[validTo] = 
				lead(
					CAST(case when [syncCreatedAt] IS NOT NULL then [syncCreatedAt] when [syncDeletedAt] IS NOT NULL then [syncDeletedAt] else [syncUpdatedAt] end AS DATETIME2)
					, 1
					, CAST('9999-12-31T23:59:59.9999999Z' AS DATETIME2)
				) OVER (
					PARTITION BY id,
						year,
						month,
						sequence,
						businessUnitDescription
					ORDER BY (
						case
							when [syncCreatedAt] IS NOT NULL then [syncCreatedAt]
							when [syncDeletedAt] IS NOT NULL then [syncDeletedAt]
							else [syncUpdatedAt] end)
					),
			IsSyncDeletedAtNull = CASE WHEN syncDeletedAt is null then 1 else 0 end,
			Thunders = 'Comercial'
		FROM BookComercial.dbo.operation_history

		UNION ALL

		SELECT *,
			[validFrom] = COALESCE(
				CAST(case
					when [syncCreatedAt] IS NOT NULL then [syncCreatedAt]
					when [syncDeletedAt] IS NOT NULL then [syncDeletedAt]
					else [syncUpdatedAt]
				end AS DATETIME2),
				CAST('0001-01-01T00:00:00.000Z' AS DATETIME2)
		
			),
			[validTo] = 
				lead(
					CAST(case when [syncCreatedAt] IS NOT NULL then [syncCreatedAt] when [syncDeletedAt] IS NOT NULL then [syncDeletedAt] else [syncUpdatedAt] end AS DATETIME2)
					, 1
					, CAST('9999-12-31T23:59:59.9999999Z' AS DATETIME2)
				) OVER (
					PARTITION BY id,
						year,
						month,
						sequence,
						businessUnitDescription
					ORDER BY (
						case
							when [syncCreatedAt] IS NOT NULL then [syncCreatedAt]
							when [syncDeletedAt] IS NOT NULL then [syncDeletedAt]
							else [syncUpdatedAt] end)
					),
			IsSyncDeletedAtNull = CASE WHEN syncDeletedAt is null then 1 else 0 end,
			Thunders = 'Indra'
		FROM BookIndra.dbo.operation_history
	)
	insert into Modelo.dbo.proc_operation_history select * from base;





	CREATE NONCLUSTERED INDEX [IDX01_operation_history] ON [Modelo].[dbo].[proc_operation_history] (
		[validFrom] ASC
	)

	CREATE NONCLUSTERED INDEX [IDX02_operation_history] ON [Modelo].[dbo].[proc_operation_history] (
		[validTo] ASC
	)

	CREATE NONCLUSTERED INDEX [IDX03_operation_history] ON [Modelo].[dbo].[proc_operation_history] (
		IsSyncDeletedAtNull ASC
	)

	CREATE NONCLUSTERED INDEX [IDX04_operation_history] ON [Modelo].[dbo].[proc_operation_history] (
		Thunders ASC
	)
END
