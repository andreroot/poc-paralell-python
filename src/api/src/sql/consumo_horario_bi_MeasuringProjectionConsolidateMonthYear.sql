IF OBJECT_ID('tempdb..#TempTable') IS NOT NULL
BEGIN
   DROP TABLE #TempTable
END
-- Note que foi colocado um 'Físico as TipoContrato' porque a procedure não está funcionando isso...
SELECT * INTO #TempTable
FROM (
	SELECT 
			A.parent_id,
			A.[subGroup.description],
			A.[modality.description],
			A.[peak],	
			A.[offPeak],
			B.[company.name],
			B.[address.state],
			B.[connection.name]
	FROM [TREINAMENTO].[lkok].[demands] AS A
	LEFT JOIN [TREINAMENTO].[lkok].[loadPhysicalAssets_details] AS B ON B.id = A.parent_id 
	--where B.[company.name] like '%ORIGINAL INDIANA COMERCIO%'
	) TempTable

--Select * from #TempTable where [company.name] = 'ABC SISTEMA DE TRANSPORTE SPE S.A.'
--Cria uma tabela geral UNION para o histórico
IF OBJECT_ID('tempdb..#TempTable1') IS NOT NULL
BEGIN
   DROP TABLE #TempTable1
END
-- Note que foi colocado um 'Físico as TipoContrato' porque a procedure não está funcionando isso...
SELECT * INTO #TempTable1
FROM (
	SELECT  
			B.parent_id,
			B.[subGroup.description],
			B.[modality.description],
			B.[peak],	
			B.[offPeak],
			B.[company.name],
			B.[address.state],
			B.[connection.name],
			C.[subMarket],
			C.[EnergySource],
			C.[SavingsPercentage],
			C.[parent_code],
			C.parent_start,
			C.parent_end
	FROM [TREINAMENTO].[lkok].[guaranteed_savings_charges_services_physicalAssets] AS C
	LEFT JOIN #TempTable AS B ON C.physicalAssetId = B.parent_id 
	--where B.parent_id  IN ('1452449c-029c-43df-876f-58fceee4bed4','50d6b72d-dc2e-414b-9d3d-fae83d9141c6' )
	--where B.[company.name] = 'ORIGINAL INDIANA COMERCIO DE VEICULOS, PECAS E SERVICOS SA'
	--where parent_code = 'EG091-25'
	) TempTable1


--Cria uma tabela geral UNION para o histórico
IF OBJECT_ID('tempdb..#TempTable2') IS NOT NULL
BEGIN
   DROP TABLE #TempTable2
END
-- Note que foi colocado um 'Físico as TipoContrato' porque a procedure não está funcionando isso...
SELECT * INTO #TempTable2
FROM (
	select 
		A.id,
		B.guaranteedSavingsContractId,
		A.Sequence,
		A.Year,
		A.Month,
		A.PeriodReference,
		A.isActive,
		A.code,
		A.Party,
		A.cnpjParty,
		A.counterParty,
		A.cnpjCounterParty,
		A.startPeriod,
		A.endPeriod,
		A.managementDealId,
		A.businessUnit,
		A.physicalAssetsCount,
		A.savingsPercentageContracted,
		A.createdDate,
		B.chargeConfiguration,
		B.physicalAssets,
		B.[chargeConfiguration.charges]
	FROM [TREINAMENTO].[lkok].[monthview] AS A
	LEFT JOIN [TREINAMENTO].[lkok].[guaranteed_savings_charges_services] as B
	ON A.id = B.guaranteedSavingsContractId AND A.sequence = B.sequence and A.periodReference between B.[start] and B.[end]
	--where isactive = '1' --and physicalAssetsCount = '1'
	--and code = 'EG091-25'

	) TempTable2

IF OBJECT_ID('tempdb..#TempTable3') IS NOT NULL
BEGIN
   DROP TABLE #TempTable3
END
-- Note que foi colocado um 'Físico as TipoContrato' porque a procedure não está funcionando isso...
SELECT * INTO #TempTable3
FROM (
	select 
		A.guaranteedSavingsContractId,
		A.Sequence,
		A.Year,
		A.Month,
		A.PeriodReference,
		A.isActive,
		A.code,
		A.Party,
		A.cnpjParty,
		A.counterParty,
		A.cnpjCounterParty,
		A.startPeriod,
		A.endPeriod,
		A.managementDealId,
		A.businessUnit,
		A.physicalAssetsCount,
		A.savingsPercentageContracted as ERROR_savingsPercentageContracted,
		A.createdDate,
		A.chargeConfiguration,
		A.physicalAssets,
		A.[chargeConfiguration.charges],
		B.savingsPercentage,
		B.physicalAssetId,
		B.subMarket,	
		B.energySource,
		B.guaranteedSavingsContractServicePeriodId
	FROM #TempTable2 AS A
	LEFT JOIN [TREINAMENTO].[lkok].[guaranteed_savings_charges_services_physicalAssets] as B
	ON A.id = B.parent_guaranteedSavingsContractId AND A.sequence = B.parent_sequence and A.periodReference between B.[parent_start] and B.[parent_end]
	--where code = 'EG070-24'
	) TempTable3

IF OBJECT_ID('tempdb..#TempTable4') IS NOT NULL
BEGIN
   DROP TABLE #TempTable4
END
-- Note que foi colocado um 'Físico as TipoContrato' porque a procedure não está funcionando isso...
SELECT * INTO #TempTable4
FROM (
	SELECT A.*,
		   B.[subGroup.description]	,
			B.[modality.description],	
			B.[peak]	,
			B.[offPeak],
			B.[address.state]	,
			B.[connection.name]
	FROM #TempTable3 AS A
	LEFT JOIN #TempTable1 AS B ON B.parent_id = A.physicalAssetId and A.periodReference between B.parent_start and B.parent_end
	) TempTable4

--SELECT * FROM #TempTable4
--SELECT * FROM TREINAMENTO.[lkok].[MeasuringPoint] where id in ('6aa5f585-7e7f-4900-a005-43e1194173ae')
--SELECT * FROM TREINAMENTO.[lkok].[MeasuringPoint_details] where id in ('d8c01b8b-fd7c-45ab-88fd-28382e64809b','dabbc320-efe5-45b4-9c24-79541e23990e')
--SELECT * FROM TREINAMENTO.[lkok].[MeasuringPoint_associatedAssets] where [physicalAsset.id] in ('1452449c-029c-43df-876f-58fceee4bed4','50d6b72d-dc2e-414b-9d3d-fae83d9141c6')
--SELECT * FROM TREINAMENTO.[lkok].[MeasuringProjectionConsolidateMonthYear]  where id in ('d8c01b8b-fd7c-45ab-88fd-28382e64809b','dabbc320-efe5-45b4-9c24-79541e23990e')

IF OBJECT_ID('tempdb..#TempTable5') IS NOT NULL
BEGIN
   DROP TABLE #TempTable5
END
-- Note que foi colocado um 'Físico as TipoContrato' porque a procedure não está funcionando isso...
SELECT * INTO #TempTable5
FROM (
	SELECT B.ID as [MeasuringPoint.id],
		  A.[physicalAsset.id],
		  A.[physicalAsset.name],
		  A.[period.endDate.year],	
		  A.[period.endDate.month]	,
		  A.[period.endDate.day],
		  B.type,
		  B.startDate,
		  B.endDate	,
		  B.collectionMode,	
		  B.consumerUnity,
		  C.[consumptionProjection.formulaId],
		  C.[consumptionProjection.months]	,
		  C.[demandProjection.formulaId],
		  C.[demandProjection.months]
	FROM TREINAMENTO.[lkok].[MeasuringPoint_associatedAssets] A
	LEFT JOIN TREINAMENTO.[lkok].[MeasuringPoint] B ON A.parent_id = B.id
	LEFT JOIN TREINAMENTO.[lkok].[MeasuringPoint_details] C ON C.id = B.id
	--where A.[physicalAsset.id] = '6aa5f585-7e7f-4900-a005-43e1194173ae'
	) TempTable5

IF OBJECT_ID('tempdb..#TempTable6') IS NOT NULL
BEGIN
   DROP TABLE #TempTable6
END
-- Note que foi colocado um 'Físico as TipoContrato' porque a procedure não está funcionando isso...
SELECT * INTO #TempTable6
FROM (
	SELECT * FROM #TempTable4 AS A
	left join #TempTable5 AS B on  B.[physicalAsset.id]= A.[physicalAssetid]
	left join [TREINAMENTO].[lkok].[MeasuringProjectionConsolidateMonthYear] AS C on  b.[MeasuringPoint.id] = c.id AND A.PeriodReference = C.[period]
	--where counterParty like '%ABC%'
	) TempTable6

;
WITH cte as (
select DISTINCT 
CODE
,submarketDescription
,energySourceDescription  
from Book.Book.proc_InformacaoComercial_table6 
where operationType = 'Economia Garantida'
)

SELECT 
A.*
,B.submarketDescription
,B.energySourceDescription
FROM #TempTable6 A
left join cte B
on A.code = B.code