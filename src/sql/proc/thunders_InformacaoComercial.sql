USE [Book]
GO

/****** Object:  StoredProcedure [Book].[InformacaoComercial]    Script Date: 28/02/2025 08:39:38 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO










ALTER PROCEDURE [Book].[InformacaoComercial]  @DataInicio DATE, @Curva varchar (20) = 'Oficial'
AS


--declare @DataInicio DATE
--declare @Curva varchar (20)
--
--Set @DataInicio = NULL
--set @Curva = 'Oficial'

DECLARE @cols  AS NVARCHAR(MAX)='';
DECLARE @cols_cast  AS NVARCHAR(MAX)='';
DECLARE @cols_cast_creat  AS NVARCHAR(MAX)='';
DECLARE @query  AS NVARCHAR(MAX)='';
DECLARE @primeiraData  Date = (SELECT MIN(data) from Book.Curva.VW_Curva_Fwd where curva = @Curva);

DECLARE @load_datetime AS DATETIME2 = GETUTCDATE();


DECLARE @DataFWD DATE
	SET @DataFWD= CASE WHEN (@DataInicio is null) THEN (SELECT max(data) from Book.Curva.VW_Curva_Fwd where curva = @Curva) 
				 WHEN (@datainicio < @primeiraData) THEN @primeiraData
				ELSE @DataInicio END
	WHILE((SELECT DISTINCT Data FROM Book.Curva.VW_Curva_Fwd WHERE Data = @DataFWD and Curva=@Curva) IS NULL)
	BEGIN 
		SET @DataFWD = DATEADD(DD,-1,@DataFWD);
	END;
	SET NOCOUNT ON;

--DECLARE @DataFWD varchar (20) = '2022-01-03'
--DECLARE @Curva varchar (20) = 'Oficial'

-- UMA STRING COM TODOS OS POSSIVEIS CAMPOS PARA O TIPO @tipo
SELECT @cols = @cols + QUOTENAME(campo) + CHAR(13)+CHAR(10)+  ',' 
FROM (
	SELECT DISTINCT campo 
	FROM book.[Book].[AjustesOperationPivot]
) AS Campos
-- REMOVE A VIRGULA DO FINAL DA STRING
SELECT @cols = SUBSTRING(@cols, 0, LEN(@cols))
print(@cols)


--CRIA UM STRING QUE CORRIGE O TIPO DOS CAMPOS DAS COLUNAS
SELECT @cols_cast = @cols_cast + 'CAST(' + QUOTENAME(campo) + ' AS ' + tipoCampo + ') AS ' + QUOTENAME(campo) + ','
FROM (
	SELECT DISTINCT campo,tipoCampo
	FROM Book.[Book].[AjustesOperationPivot]

) AS Campos
-- REMOVE A VIRGULA DO FINAL DA STRING
SELECT @cols_cast = SUBSTRING(@cols_cast, 0, LEN(@cols_cast))
print(@cols_cast)

-- CRIA UMA STRING COM AS INFORMAÇÕES PARA GERAR A TABELA 
SELECT @cols_cast_creat  = @cols_cast_creat   + campo + '  ' + tipoCampo + CHAR(13)+CHAR(10)+ ' , '
FROM (
	SELECT DISTINCT campo,tipoCampo
	FROM BOOk.[Book].[AjustesOperationPivot]

) AS Campos
-- REMOVE A VIRGULA DO FINAL DA STRING
SELECT @cols_cast_creat  = ', '+SUBSTRING(@cols_cast_creat , 0, LEN(@cols_cast_creat ))
-- PRINTA NO FORMATO
print(@cols_cast_creat)


--ESSA query FAZ O PIVOT E ALTERA O TIPO DAS COLUNAS
SET @query = '       
				  SELECT	code
							,Thunders
							,isTrading		
							,year			
							,month	
							, ' + @cols_cast + '
				   FROM 
				 (
					 select code
							,Thunders
							,isTrading		
							,year			
							,month	
							--, DataInsert
							, Valor
							, Campo
							
					from Book.[Book].[AjustesOperationPivot]
				 ) x
				 PIVOT 
				 (
					 MIN(Valor)
					 FOR [campo] IN (' + @cols + ')
				) AS pivot_table    

				'

------------------------------------------------------------------------------------------------------------------------------------------
--										Criando a Tabela com colunas dinâmicas
------------------------------------------------------------------------------------------------------------------------------------------
-- É CRIADO A TABELA QUE GUARDA A INFORMAÇÃO PIVOTEADA
IF OBJECT_ID('tempdb..#Doodles') IS NOT NULL
            BEGIN
                DROP TABLE #Doodles
            END
CREATE TABLE #Doodles  (code			varchar(250) NULL)

DECLARE @qry nvarchar (MAX)
SET @qry = 'ALTER TABLE #Doodles ADD
		 Thunders		VARCHAR(250) NULL
		, isTrading		BIT NOT NULL
		, year			INT NOT NULL
		, month			INT NOT NULL' + @cols_cast_creat
Exec(@qry)

INSERT #Doodles EXECUTE (@query)

------------------------------------------------------------------------------------------------------------------------------------------

-- É CRIADO A TABELA QUE GUARDA A INFORMAÇÃO PIVOTEADA
--IF OBJECT_ID('tempdb..#Doodles') IS NOT NULL
--            BEGIN
--                DROP TABLE #Doodles
--            END
--CREATE TABLE #Doodles  (
--				code			varchar(250) NULL
--				,Thunders		varchar(250) NULL
--				,isTrading		bit not NULL
--				,year			int not NULL
--				,month			int not NULL
		
---				--Colar aqui o resultado do print(@cols_cast_creat)
---				--Ele gera as informaçoes das colunas que serão criadas

--				, precoFlex  float
--				, contatoTelefone  int
--				, condicaoEspecial  [varchar](250)
--				, spreadComercial  float
--				, tempoParaFechar  int
--				, precoModulacao  float
--				, justificativa  [varchar](250)
--				, PrecoSE_CO  float
--				, proposalThunders  [varchar](250)
--				, precoSwap  float
--				, codeSCP  [varchar](250)
--				, clienteNovo  [bit]
--				, fee  float
--				, precoInformado  float
--				, relacionamento  [varchar](250)
--				, nomeConsultoria  [varchar](250)
--				, consultoriaouClienteFinal  [varchar](250)
--				, precoDescolocamentoFluxodeCaixa  float
--				, formaContato  [varchar](250)
--				, precoSazo  float				
--)
--INSERT #Doodles EXECUTE (@query)

------------------------------------------------------------------------------------------------------------------------------------------

IF OBJECT_ID('tempdb..#Union') IS NOT NULL
BEGIN
   DROP TABLE #Union
END

	SELECT * INTO #Union
	FROM(
			SELECT *, 'Safira' as Thunders FROM book.dbo.operation
			UNION ALL
			SELECT *, 'Comercial' as Thunders FROM bookcomercial.dbo.operation
			UNION ALL
			SELECT *, 'Indra' as Thunders FROM bookindra.dbo.operation
		) AS #Union
		-- SELECT * FROM #UNION 

--Union com Filtro
IF OBJECT_ID('tempdb..#UnionFilter') IS NOT NULL
BEGIN
   DROP TABLE #UnionFilter
END

	SELECT * INTO #UnionFilter
	FROM(
			SELECT * FROM #Union 
			WHERE isActive = 1
			AND businessunitdescription IN ('Varejista', 'Trading')
		) AS #UnionFilter
		  --SELECT * FROM #UnionFilter where code = 'EG091-24'

---------------------------------------------------------------------------------------------------------------------------------------------
--								Pega todas as colunas da tabela AjusterOperation e adiciona um B. antes delas.
---------------------------------------------------------------------------------------------------------------------------------------------
DECLARE @cols_cast_creat_B  AS NVARCHAR(MAX)='';
-- CRIA UMA STRING COM AS INFORMAÇÕES PARA GERAR A TABELA 
SELECT @cols_cast_creat_B  = @cols_cast_creat_B   + campo + '  ' + CHAR(13)+CHAR(10)+ ', B.'
FROM (
	SELECT DISTINCT campo
	FROM BOOk.[Book].[AjustesOperationPivot]

) AS Campos

-- REMOVE A VIRGULA DO FINAL DA STRING
SELECT @cols_cast_creat_B  = ', B.'+SUBSTRING(@cols_cast_creat_B , -3, LEN(@cols_cast_creat_B ))
-- PRINTA NO FORMATO
print(@cols_cast_creat_B)

---------------------------------------------------------------------------------------------------------------------------------------------
--													Fazendo um LEFT Join com a Tabela Union 
---------------------------------------------------------------------------------------------------------------------------------------------
DECLARE @query_b AS NVARCHAR(MAX)='';
SET @query_b = 'IF OBJECT_ID(''tempdb..##UnionAjusted_1234'') IS NOT NULL
			  BEGIN
			     DROP TABLE ##UnionAjusted_1234
			END
		SELECT * INTO ##UnionAjusted_1234 FROM (SELECT
		A.[code] 
		,A.[Thunders]
		,A.[sequence] 
		,A.[operationType] 
		,A.[tradeType] 
		,A.[version] 
		,A.[primaryOperationId] 
		,A.[primaryOperationCode] 
		,A.[primaryOperationSequence] 
		,A.[isTrading] 
		,A.[isServices] 
		,A.[isGeneration] 
		,A.[businessUnitDescription] 
		,A.[isActive] 
		,A.[partyId] 
		,A.[partyCNPJ] 
		,A.[partyName] 
		,A.[partyAlias] 
		,A.[partyAgentAcronym] 
		,A.[partyAgentCode] 
		,A.[partyProfileCode] 
		,A.[partyProfileDescription] 
		,A.[counterpartyId] 
		,A.[counterpartyCNPJ] 
		,A.[counterpartyName] 
		,A.[counterpartyAlias] 
		,A.[counterpartyAgentAcronym] 
		,A.[counterpartyAgentCode] 
		,A.[counterpartyProfileCode] 
		,A.[counterpartyProfileDescription] 
		,A.[counterPartyIsGroupCompany] 
		,A.[userOperatorName]     
		,A.[submarketDescription] 
		,A.[energySourceDescription] 
		,A.[priceTypeDescription] 
		,A.[startDate] 
		,A.[endDate] 
		,A.[contractedVolumeMwm] 
		,A.[contractedVolumeMwh] 
		,A.[seasonalityVolumeMwh] 
		,A.[seasonalityVolumeMwm] 
		,A.[finalVolumeMwh] 
		,A.[finalVolumeMwm] 
		,A.[basePrice] 
		,A.[price] 
		,A.[nominalPrice] 
		,A.[mtm] 
		,A.[retusd] 
		,A.[classifications] 
		,A.[userCreatedName] 
		,A.[createdAt] 
		,A.[userModifiedName] 
		,A.[modifiedAt] 
		,A.[userDeletedName] 
		,A.[deletedAt] 
		,A.[userBackofficeName] 
		,A.[userCommercialName] 
		,A.[origin] 
		,A.[bbceCode] 
		,A.[hasFlexibility] 
		,A.[isFlexibilityLoadCurve] 
		,A.[isFlexibilityByPeriod] 
		,A.[flexibilityPercentageBottom] 
		,A.[flexibilityPercentageTop] 
		,A.[hasSeasonality] 
		,A.[isSeasonalityByPeriod] 
		,A.[seasonalityPercentageBottom] 
		,A.[seasonalityPercentageTop] 
		,A.[hasModulation] 
		,A.[isModulationLoadCurve] 
		,A.[hasDefaultFinancialFlow] 
		,A.[hasReadjustment] 
		,A.[reajustmentIndex] 
		,A.[readjustmentBaseDate] 
		,A.[readjustmentFirstDate] 
		,A.[hasGuarantee] 
		,A.[guaranteeValue] 
		,A.[guaranteeDueDate] 
		,A.[guaranteeTypes] 
		,A.[hasRepresentativeFactor] 
		,A.[representativeFactorPercent] 
		,A.[losses] 
		,A.[cceeContractCode] 
		,A.[needApportionment] 
		,A.[spread] 
		,A.[floor] 
		,A.[ceiling] 
		,A.[billingStatus] 
		,A.[aprovalStatusId] 
		,A.[aprovalStatusDescription] 
		,A.[year] 
		,A.[month] 
		,A.[startDay] 
		,A.[endDay] 
		,A.[id] 
		,A.[operationTypeId] 
		,A.[_Link] 
		,A.[priceVariableTypeId] 
		,A.[priceVariableIndex] 
		,A.[isDraft] 
		,A.[userOperatorCouterPartyName] 
		,A.[portfolios] 
		,A.[consentingIntervenerCompanyName] 
		,A.[proposalCode] 
		,A.[proposalId] 
		,A.[basePriceWithReadjustment] 
		,A.[parentOperationId] 
		,A.[availableToUpdateApportionment] 
		' + @cols_cast_creat_B + '
									
		,CAST(CAST(A.year AS varchar(250)) + ''-'' + CAST(A.month AS varchar(250))+''-01'' AS  DATE) as DataFornecimento
		,CAST(createdAt as DATE) as DataCriacao
		FROM #UnionFilter as A
	LEFT JOIN #Doodles as B
	ON	A.year = B.year 
	and A.month = B.month
	and A.[Thunders] COLLATE DATABASE_DEFAULT = B.[Thunders] COLLATE DATABASE_DEFAULT
	and	A.code COLLATE DATABASE_DEFAULT = B.code COLLATE DATABASE_DEFAULT ---Coreção do erro de versão do SQL
	--where A.year = 2022 and A.month = 12 and A.createdAt > ''2021-19-10'' and A.code = ''VC013-22''
	) UnionAjusted
	' 
-- Executando a Query
EXEC (@query_b)

IF OBJECT_ID('tempdb..#UnionAjusted') IS NOT NULL
			  BEGIN
			     DROP TABLE #UnionAjusted
			END
			SELECT * INTO #UnionAjusted FROM(SELECT * FROM ##UnionAjusted_1234) UnionAjusted

DROP TABLE ##UnionAjusted_1234

--select * from #UnionAjusted  where code = 'CC007-20'

--select * from book.Curva.PLD_Oficial
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--										Tabela temporária para selecionar a Curva Fwd
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

	IF OBJECT_ID('tempdb..#CurvaFwd') IS NOT NULL
	BEGIN
		DROP TABLE #CurvaFwd
	END
 
	SELECT * INTO #CurvaFwd
	FROM (
		SELECT *,
			(CASE WHEN Submercado = 'SE' THEN 'Sudeste'
				WHEN Submercado = 'S' THEN 'Sul'
				WHEN Submercado = 'N' THEN 'Norte'
				WHEN Submercado = 'NE' THEN 'Nordeste'
			END) AS submarketDescription,
		(CASE WHEN Fonte_Energia = 'CQ5' THEN 'Cogeração Qualificada 50%'
			WHEN Fonte_Energia = 'CQ1' THEN 'Cogeração Qualificada 100%'
			WHEN Fonte_Energia = 'Convencional' THEN Fonte_Energia
			WHEN Fonte_Energia = '0% Incent.' THEN 'Incentivada 0%'
			WHEN Fonte_Energia = '100% Incent.' THEN 'Incentivada 100%'
			WHEN Fonte_Energia = '50% Incent.' THEN 'Incentivada 50%'
			WHEN Fonte_Energia = '80% Incent.' THEN 'Incentivada 80%'
			WHEN Fonte_Energia = 'INE5' THEN 'Incentivada Não Especial 50%'
			WHEN Fonte_Energia = 'INE1' THEN 'Incentivada Não Especial 100%'
			END) AS energySourceDescription
		FROM [Book].[Curva].[VW_Curva_Fwd] AS C 
		WHERE 
				C.Curva = @Curva -- Inserir Curva
		--AND 	C.[Data]= @DataFWD -- Inserir DataFwd 
			
	) CurvaFwd

--AND (CASE WHEN A.submarketDescription = ''Sudeste'' THEN ''SE''
--			WHEN A.submarketDescription = ''Sul'' THEN ''S''
--			WHEN A.submarketDescription = ''Norte'' THEN ''N''
--			WHEN A.submarketDescription = ''Nordeste'' THEN ''NE''
--		END)=D.Submercado
--	AND D.Data= A.DataCriacao
--	AND (CASE WHEN energySourceDescription = ''Cogeração Qualificada 50%'' THEN ''CQ5''
--			WHEN energySourceDescription = ''Convencional'' THEN energySourceDescription
--			WHEN energySourceDescription = ''Incentivada 0%'' THEN ''0% Incent.'' 
--			WHEN energySourceDescription = ''Incentivada 100%''THEN ''100% Incent.''
--			WHEN energySourceDescription = ''Incentivada 50%'' THEN ''50% Incent.''
--		END) = D.Fonte_Energia		   

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--										Tabela temporária para selecionar a Curva Fwd 8º Dia Útil
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	IF OBJECT_ID('tempdb..#CurvaFwdTableFlexibility') IS NOT NULL
	BEGIN
	   DROP TABLE #CurvaFwdTableFlexibility
	END
	SELECT * INTO #CurvaFwdTableFlexibility
	FROM(
	--É preciso fazer isso porque "preciso selecionar curvas do passado..."
		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2021-01-04')
		AND Curva = 'Oficial'
		AND Data_fwd = '2020-12-01'
		
		UNION ALL
		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2021-02-03')
		AND Curva = 'Oficial'
		AND Data_fwd = '2021-01-01'

		UNION ALL
		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2021-03-03')
		AND Curva = 'Oficial'
		AND Data_fwd = '2021-02-01'

		UNION ALL
		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2021-04-03')
		AND Curva = 'Oficial'
		AND Data_fwd = '2021-03-01'

		UNION ALL
		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2021-05-03')
		AND Curva = 'Oficial'
		AND Data_fwd = '2021-04-01'

		UNION ALL
		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2021-06-03')
		AND Curva = 'Oficial'
		AND Data_fwd = '2021-05-01'

		UNION ALL
		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2021-07-05')
		AND Curva = 'Oficial'
		AND Data_fwd = '2021-06-01'

		UNION ALL
		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2021-08-03')
		AND Curva = 'Oficial'
		AND Data_fwd = '2021-07-01'

		UNION ALL
		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2021-09-03')
		AND Curva = 'Oficial'
		AND Data_fwd = '2021-08-01'

		UNION ALL
		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2021-10-05')
		AND Curva = 'Oficial'
		AND Data_fwd = '2021-09-01'

		UNION ALL
		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2021-11-04')
		AND Curva = 'Oficial'
		AND Data_fwd = '2021-10-01'

		UNION ALL
		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2021-12-03')
		AND Curva = 'Oficial'
		AND Data_fwd = '2021-11-01'

		UNION ALL
		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2022-01-05')
		AND Curva = 'Oficial'
		AND Data_fwd = '2021-12-01'

		UNION ALL
		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2022-02-04')
		AND Curva = 'Oficial'
		AND Data_fwd = '2022-01-01'

		UNION ALL
		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2022-03-04')
		AND Curva = 'Oficial'
		AND Data_fwd = '2022-02-01'

		UNION ALL
		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2022-04-05')
		AND Curva = 'Oficial'
		AND Data_fwd = '2022-03-01'

		UNION ALL
		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2022-05-04')
		AND Curva = 'Oficial'
		AND Data_fwd = '2022-04-01'
		UNION ALL 

		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2022-06-03')
		AND Curva = 'Oficial'
		AND Data_fwd = '2022-05-01'
		UNION ALL 

		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2022-07-05')
		AND Curva = 'Oficial'
		AND Data_fwd = '2022-06-01'
		UNION ALL 

		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2022-08-10')
		AND Curva = 'Oficial'
		AND Data_fwd = '2022-07-01'
		UNION ALL

		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2022-09-13')
		AND Curva = 'Oficial'
		AND Data_fwd = '2022-08-01'
		UNION ALL 

		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2022-10-13')
		AND Curva = 'Oficial'
		AND Data_fwd = '2022-09-01'
		UNION ALL 
		
		SELECT *
		FROM #CurvaFwd
		WHERE Data IN ('2022-11-10')
		AND Curva = 'Oficial'
		AND Data_fwd = '2022-10-01'
		UNION ALL 
		

		SELECT *
		FROM #CurvaFwd
		WHERE Data IN (SELECT MAX(Data) FROM #CurvaFwd WHERE Curva = 'Oficial')
) CurvaFwdTableFlexibility


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--								Tabela temporária para selecionar o PLD Oficial
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

	IF OBJECT_ID('tempdb..#PLDOficial') IS NOT NULL
	BEGIN
		DROP TABLE #PLDOficial
	END
 
	SELECT * INTO #PLDOficial
	FROM ( 
		SELECT *,
			(CASE WHEN Submercado = 'SE' THEN 'Sudeste'
			WHEN Submercado = 'S' THEN 'Sul'
			WHEN Submercado = 'N' THEN 'Norte'
			WHEN Submercado = 'NE' THEN 'Nordeste'
			END) AS submarketDescription
		FROM [Book].[Curva].[PLD_Oficial] AS C 
		WHERE c.DataInsert <= @DataFWD-- Inserir DataFwd
	) PLDOficial



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--											Pega todas as colunas da tabela AjusterOperation e adiciona um A. antes delas.
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
DECLARE @cols_cast_creat_A  AS NVARCHAR(MAX)='';
-- CRIA UMA STRING COM AS INFORMAÇÕES PARA GERAR A TABELA 
SELECT @cols_cast_creat_A  = @cols_cast_creat_A   + campo + '  ' + CHAR(13)+CHAR(10)+ ', A.'
FROM (
	SELECT DISTINCT campo
	FROM BOOk.[Book].[AjustesOperationPivot]

) AS Campos

-- REMOVE A VIRGULA DO FINAL DA STRING
SELECT @cols_cast_creat_A  = ', A.'+SUBSTRING(@cols_cast_creat_A , -3, LEN(@cols_cast_creat_A ))

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--											Criando a variável para sempre pegar todas as colunas novas e adiciona informações 
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
DECLARE @query_a AS NVARCHAR(MAX)='';
SET @query_a = 'IF OBJECT_ID(''tempdb..##TempOperation_1234'') IS NOT NULL
BEGIN
   DROP TABLE ##TempOperation_1234
END
SELECT * INTO ##TempOperation_1234
FROM(SELECT
		A.[code] 
		,A.[Thunders]
		,A.[sequence] 
		,A.[operationType] 
		,A.[tradeType] 
		,A.[version] 
		,A.[primaryOperationId] 
		,A.[primaryOperationCode] 
		,A.[primaryOperationSequence] 
		,A.[isTrading] 
		,A.[isServices] 
		,A.[isGeneration] 
		,A.[businessUnitDescription] 
		,A.[isActive] 
		,A.[partyId] 
		,A.[partyCNPJ] 
		,A.[partyName] 
		,A.[partyAlias] 
		,A.[partyAgentAcronym] 
		,A.[partyAgentCode] 
		,A.[partyProfileCode] 
		,A.[partyProfileDescription] 
		,A.[counterpartyId] 
		,A.[counterpartyCNPJ] 
		,A.[counterpartyName] 
		,A.[counterpartyAlias] 
		,A.[counterpartyAgentAcronym] 
		,A.[counterpartyAgentCode] 
		,A.[counterpartyProfileCode] 
		,A.[counterpartyProfileDescription] 
		,A.[counterPartyIsGroupCompany] 
		,A.[userOperatorName]     
		,A.[submarketDescription] 
		,A.[energySourceDescription] 
		,A.[priceTypeDescription] 
		,A.[startDate] 
		,A.[endDate] 
		,A.[contractedVolumeMwm] 
		,A.[contractedVolumeMwh] 
		,A.[seasonalityVolumeMwh] 
		,A.[seasonalityVolumeMwm] 
		,A.[finalVolumeMwh] 
		,A.[finalVolumeMwm] 
		,A.[basePrice] 
		,A.[price] 
		,A.[nominalPrice] 
		,A.[mtm] 
		,A.[retusd] 
		,A.[classifications] 
		,A.[userCreatedName] 
		,A.[createdAt]
		,CASE 
			WHEN (A.partyCNPJ=A.counterpartyCNPJ) THEN ''IC''
			WHEN (A.partyCNPJ<>A.counterpartyCNPJ) THEN 
				CASE WHEN (a.counterpartyCNPJ IN (''09.495.582/0001-07''
													,''10.938.805/0001-34''
													,''11.482.752/0001-52''
													,''31.562.321/0001-03''
													,''32.312.466/0001-19''
													,''35.417.904/0001-00''
													,''38.349.131/0001-51''
												)) THEN ''Negocio Interno''
			ELSE ''Negocio Externo'' 
				END 
		END AS TypeBusiness
		,A.[userModifiedName] 
		,A.[modifiedAt] 
		,A.[userDeletedName] 
		,A.[deletedAt] 
		,A.[userBackofficeName] 
		,A.[userCommercialName] 
		,A.[origin] 
		,A.[bbceCode] 
		,A.[hasFlexibility] 
		,A.[isFlexibilityLoadCurve] 
		,A.[isFlexibilityByPeriod] 
		,A.[flexibilityPercentageBottom] 
		,A.[flexibilityPercentageTop] 
		,A.[hasSeasonality] 
		,A.[isSeasonalityByPeriod] 
		,A.[seasonalityPercentageBottom] 
		,A.[seasonalityPercentageTop] 
		,A.[hasModulation] 
		,A.[isModulationLoadCurve] 
		,A.[hasDefaultFinancialFlow] 
		,A.[hasReadjustment] 
		,A.[reajustmentIndex] 
		,A.[readjustmentBaseDate] 
		,A.[readjustmentFirstDate] 
		,A.[hasGuarantee] 
		,A.[guaranteeValue] 
		,A.[guaranteeDueDate] 
		,A.[guaranteeTypes] 
		,A.[hasRepresentativeFactor] 
		,A.[representativeFactorPercent] 
		,A.[losses] 
		,A.[cceeContractCode] 
		,A.[needApportionment] 
		,A.[spread] 
		,A.[floor] 
		,A.[ceiling] 
		,A.[billingStatus] 
		,A.[aprovalStatusId] 
		,A.[aprovalStatusDescription] 
		,A.[year] 
		,A.[month] 
		,A.[startDay] 
		,A.[endDay] 
		,A.[id] 
		,A.[operationTypeId] 
		,A.[_Link] 
		,A.[priceVariableTypeId] 
		,A.[priceVariableIndex] 
		,A.[isDraft] 
		,A.[userOperatorCouterPartyName] 
		,A.[portfolios] 
		,A.[consentingIntervenerCompanyName] 
		,A.[proposalCode] 
		,A.[proposalId] 
		,A.[basePriceWithReadjustment] 
		,A.[parentOperationId] 
		,A.[availableToUpdateApportionment]
		,B.[DataInsert] AS PLD_DataInsert
		,B.[Preco] AS PLD_Preco
		,C.[Data] AS FWD_Data
		,C.[Curva] AS FWD_Curva
		,C.[Preco_Energia] AS FWD_Preco_Energia
		,C.[Preco_PLD] AS FWD_Preco_PLD
		,C.[Agio] AS FWD_Preco_Agio

		,D.[Data] AS FWD_createdAt_Data
		,D.[Curva] AS FWD_createdAt_Curva
		,D.[Preco_Energia] AS FWD_createdAt_Preco_Energia
		,D.[Preco_PLD] AS FWD_createdAt_Preco_PLD
		,D.[Agio] AS FWD_createdAt_Preco_Agio


		,E.[Data] AS FWD_Flex_Data
		,E.[Curva] AS FWD_Flex_Curva
		,E.[Preco_Energia] AS FWD_Flex_Preco_Energia
		,E.[Preco_PLD] AS FWD_Flex_Preco_PLD
		,E.[Agio] AS FWD_Flex_Preco_Agio

		' + @cols_cast_creat_A +'
		,Book.fn_Resultado(A.price, C.Preco_Energia, B.Preco, A.ceiling, A.floor, C.Preco_PLD, A.Spread, A.tradetype, A.priceTypeDescription, A.finalVolumeMwh)	as Resultado
		,Book.fn_NotaFiscal(A.price, C.Preco_Energia, B.Preco, A.ceiling, A.floor, C.Preco_PLD, A.Spread, A.tradetype, A.priceTypeDescription, A.finalVolumeMwh) as NetNotaFiscal
		,(A.SpreadComercial * A.FinalVolumeMwh) AS ResultadoSpread
		,A.DataFornecimento
		,A.DataCriacao
		, CASE
			WHEN ISNULL(A.precoSwap, 0)+ ISNULL(A.precoFlex, 0)+  ISNULL(A.precoSazo, 0) +  + ISNULL(A.precoModulacao, 0) +ISNULL(A.precoInformado, A.basePrice)  + ISNULL(A.spreadComercial, 0) = Baseprice then ''Verdadeiro'' 
        ELSE ''Falso''
		END as validaPrecoContrato
		FROM #UnionAjusted as A

		LEFT JOIN #PLDOficial  AS B
		ON A.DataFornecimento = B.Data 
		AND A.submarketDescription = B.submarketDescription
		AND B.DataInsert <= ''' + Cast(@DataFWD AS NVARCHAR(14)) + ''' --Inserir DataDia
		
		LEFT JOIN #CurvaFwd AS C
			ON A.DataFornecimento = C.data_fwd
			AND A.submarketDescription = C.submarketDescription
			AND A.energySourceDescription = C.energySourceDescription
			AND C.Data= ''' + Cast(@DataFWD AS NVARCHAR(14)) + ''' -- Inserir DataFwd
			AND C.Curva = ''' + Cast(@Curva AS NVARCHAR(14)) + '''  -- Inserir Curva

		LEFT JOIN #CurvaFwd AS D
				ON A.DataFornecimento = D.data_fwd
				AND A.submarketDescription = D.submarketDescription
				AND A.energySourceDescription = D.energySourceDescription
				AND D.Data= A.DataCriacao -- Inserir DataFwd
				AND D.Curva = ''' + Cast(@Curva AS NVARCHAR(14)) + '''  -- Inserir Curva

		LEFT JOIN #CurvaFwdTableFlexibility AS E
				ON A.DataFornecimento = E.Data_Fwd
				AND A.submarketDescription = E.submarketDescription
				AND A.energySourceDescription = E.energySourceDescription		
				AND E.Data = ''' + Cast(@DataFWD AS NVARCHAR(14)) + ''' -- Inserir DataFwd   
				AND E.Curva = ''' + Cast(@Curva AS NVARCHAR(14)) + ''' -- Inserir Curva

	) AS #TempOperation_1234'

-- Exec da query
EXEC (@query_a)

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--								Criando uma Tabela Temporaria para dropar a tabela global
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#TempOperation') IS NOT NULL
			  BEGIN
			     DROP TABLE #TempOperation
			END
			SELECT * INTO #TempOperation FROM(SELECT * FROM ##TempOperation_1234) TempOperation

DROP TABLE ##TempOperation_1234

--select * from #TempOperation where YEAR=2022 AND  MONTH=12 AND code = 'VC306-22'

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--										Criando uma coluna para alterar os cpnjs nulos
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--- Criando tabela dos grupos com cnpjs nulos, e adicionando valores para eles
--IF OBJECT_ID('tempdb..#CNPJNulo') IS NOT NULL
--	BEGIN
--		DROP TABLE #CNPJNulo 
--	END

--CREATE TABLE #CNPJNulo (
--		counterpartyName NVARCHAR(250) NULL
--		, counterpartyCNPJ NVARCHAR(20) NOT NULL
--		, codagente INT NOT NULL
--		, codperfil INT NOT NULL
--		)

--INSERT INTO #CNPJNulo (counterpartyName, counterpartyCNPJ, codagente, codperfil)
--	VALUES	('GRUPO ECONOMICO - BELLOCOPO - ISOFORMA - CORTALINA'	, '22624940000194', '60470', '36052')
--			,('GRUPO ECONOMICO BELLOCOPO'							, '22624940000194', '60470', '36052')
--			,('GRUPO ECONOMICO - GRANDALL'							, '11.111.111/1111-03', '11223344', '11223344')
--			,('GRUPO ECONOMICO ACUCAREIRA QUATA S/A'				, '60855574000173', '2019', '2019')
--			,('GRUPO ECONOMICO AMCOR'								, '06559531000103', '1842', '1842')
--			,('GRUPO ECONOMICO BIMBO'								, '35402759001580', '54503', '32211')
--			,('GRUPO ECONOMICO BUNGE', '84046101000193', '1188', '1188')
--			,('GRUPO ECONOMICO CAEDU', '46377727000193', '88174', '23338')
--			,('GRUPO ECONOMICO CARAMURU', '00080671000100', '1124', '1124')
--			,('GRUPO ECONOMICO CASA RODRIGUES', '40524608000130', '92425', '26669')
--			,('GRUPO ECONOMICO CBA', '04756038000140', '58431', '36940')
--			,('GRUPO ECONOMICO CINPAL', '49656192000188', '1586', '1586')
--			,('GRUPO ECONOMICO DANA', '00253137000158', '1320', '1320')
--			,('GRUPO ECONOMICO ELDORADO', '07401436000131', '8061', '27582')
--			,('GRUPO ECONOMICO GREIF', '59320820000103', '3197', '3197')
--			,('GRUPO ECONOMICO LAFARGEHOLCIM', '60869336000117', '1148', '1148')
--			,('GRUPO ECONOMICO MAHLE', '60476884001744', '1418', '1418')
--			,('GRUPO ECONOMICO SANTA HELENA', '12150746000160', '13462', '16451')
--			,('GRUPO ECONOMICO SESPO', '50464692000105', '57398', '67230')
--			,('Grupo Econômico SSB', '08593896000171', '70757', '86209')
--			,('GRUPO ECONOMICO USINA SANTA TEREZINHA', '75717355000103', '61032', '78805')
--			,('GRUPO ECONOMICO VIDEOLAR', '04229761000766', '51946', '61119')
--			,('GRUPO ECONOMICO - SHERATON SANTOS'					, '06275513000191', '68799', '82272')
--			,('GRUPO ECONOMICO AMBEV'								, '07526557000100', '15095', '19475')
--			,('GRUPO ECONOMICO AN ADM'								, '04060874000195', '62671', '74283')
--			,('GRUPO ECONOMICO BEMIS'								, '08720614000150', '97594', '30768')
--			,('GRUPO ECONOMICO BIOSEV - LDC'						, '15527906000136', '2962', '2962')
--			,('GRUPO ECONOMICO BRASKEM'								, '42150391000170', '1104', '1104')
--			,('GRUPO ECONOMICO BTG E BRASIL PCH'					, '30306294000145', '56529', '66242')
--			,('GRUPO ECONOMICO CESARI'								, '03359531000163', '54959', '64489')
--			,('GRUPO ECONOMICO CONFAB'								, '60882628000190', '57789', '67793')
--			,('GRUPO ECONOMICO COPREL'								, '08323274000123', '3622', '3622')
--			,('GRUPO ECONOMICO COSTA RICA'							, '02993750000137', '2612', '2612')
--			,('GRUPO ECONOMICO CPFL ENERGIAS RENOVAVEIS'			, '08439659000150', '3993', '3993')
--			,('GRUPO ECONOMICO CTG'									, '03631957000124', '42', '30004')
--			,('GRUPO ECONOMICO ELETRISA'							, '11.111.111/1111-18', '11223344', '11223344')
--			,('GRUPO ECONOMICO EMAL'								, '44026037000164', '6100', '6266')
--			,('GRUPO ECONOMICO ENERCORE'							, '18416364000112', '12372', '14779')
--			,('GRUPO ECONOMICO GRANJA IPE e DIEGO CAMPOS'			, '08789279000146', '93369', '27486')
--			,('GRUPO ECONOMICO HIDRELETRICA SENS LTDA'				, '02444931000376', '2597', '2597')
--			,('GRUPO ECONOMICO HISPEX E ANOAL'						, '04956796000101', '64591', '76709')
--			,('GRUPO ECONOMICO JACAREZINHO'							, '61231478000117', '54049', '63473')
--			,('GRUPO ECONOMICO MAROMBAS GERAÇÃO'					, '30066380000128', '71014', '86728')
--			,('GRUPO ECONOMICO MESSER'								, '60619202000148', '1137', '1137')
--			,('GRUPO ECONOMICO MEXICHEM'							, '58514928000174', '1134', '1134')
--			,('GRUPO ECONOMICO MINERACAO SAO VICENTE'				, '06537334000185', '83832', '96915')
--			,('GRUPO ECONOMICO ONESUBSEA'							, '01505705000395', '61142', '72135')
--			,('GRUPO ECONOMICO PAGUE MENOS'							, '60494416000135', '52963', '62282')
--			,('GRUPO ECONOMICO PETROBRAS'							, '33000167000101', '135', '135')
--			,('GRUPO ECONOMICO PLASTIPACK'							, '01115825000114', '1212', '1212')
--			,('GRUPO ECONOMICO PLASTIPAK'							, '01115825000114', '1212', '1212')
--			,('GRUPO ECONOMICO RHODIA'								, '57507626000106', '78429', '92564')
--			,('GRUPO ECONOMICO RIO ENERGY'							, '16775973000132', '85836', '98531')
--			,('GRUPO ECONOMICO ROHDEN'								, '05959604000183', '53990', '63404')
--			,('GRUPO ECONOMICO SUZANO'								, '16404287000155', '1395', '1395')
--			,('GRUPO ECONOMICO TANGARA'								, '03573381000196', '130', '130')
--			,('GRUPO ECONOMICO TECELAGEM NUNO - NCA'				, '11.111.111/1111-39', '11223344', '11223344')
--			,('GRUPO ECONOMICO UNIAO QUIMICA'						, '60665981000541', '7782', '8240')
--			,('GRUPO ECONOMICO USINAS ITAMARATI'					, '15009178000170', '5919', '6065')
--			,('GRUPO ECONOMICO USIPAR'								, '21587696000174', '13936', '24739')

--DECLARE @cnpjs AS NVARCHAR(MAX)='';

--SET @cnpjs ='UPDATE #TempOperation
--				SET counterpartyCNPJ = tb1.counterpartyCNPJ, counterpartyAgentCode = tb1.codagente, counterpartyProfileCode = tb1.codperfil
--					FROM #CNPJNulo AS tb1
--					INNER JOIN #TempOperation AS tb2
--						ON tb1.counterpartyName COLLATE DATABASE_DEFAULT = tb2.counterpartyName COLLATE DATABASE_DEFAULT
--					WHERE tb2.counterpartyName IS NOT NULL';
--EXEC (@cnpjs)

-- DESCOMENTAR SE PRECISAR
-- -- -- -- EXEC Book.STP_I_ListaPerfil_GruposEconomicos
DECLARE @cnpjs AS NVARCHAR(MAX)='';
SET @cnpjs = '
UPDATE #TempOperation
SET 
	counterpartyCNPJ = tb1.CNPJ,
	counterpartyAgentCode = tb1.CodAgente,
	counterpartyProfileCode = tb1.CodPerfil
FROM 
	Book.Book.ListaPerfil_GruposEconomicos AS tb1
INNER JOIN
	#TempOperation AS tb2
ON
	(
		CASE 
			WHEN UPPER(tb2.counterpartyName) Collate SQL_Latin1_General_CP1253_CI_AI = ''GRUPO ECONOMICO C.E.I'' THEN ''GRUPO ECONOMICO CEI''
			WHEN UPPER(tb2.counterpartyName) Collate SQL_Latin1_General_CP1253_CI_AI = ''GRUPO ECONOMICO CONFAB - TENARIS'' THEN ''GRUPO ECONOMICO CONFAB''
			WHEN UPPER(tb2.counterpartyName) Collate SQL_Latin1_General_CP1253_CI_AI = ''GRUPO ECONOMICO PLASTIPACK'' THEN ''GRUPO ECONOMICO PLASTIPAK''
			WHEN UPPER(tb2.counterpartyName) Collate SQL_Latin1_General_CP1253_CI_AI = ''GRUPO ECONOMICO HYSPEX E ANOAL'' THEN ''GRUPO ECONOMICO HISPEX E ANOAL''
			WHEN UPPER(tb2.counterpartyName) Collate SQL_Latin1_General_CP1253_CI_AI = ''GRUPO ECONOMICO LAFARGE - HOLCIM'' THEN ''GRUPO ECONOMICO LAFARGEHOLCIM''
			WHEN UPPER(tb2.counterpartyName) Collate SQL_Latin1_General_CP1253_CI_AI = ''GRUPO ECONOMICO MEXICHEM'' THEN ''GRUPO ECONOMICO MEXICHEM BRASIL''
			ELSE UPPER(tb2.counterpartyName) Collate SQL_Latin1_General_CP1253_CI_AI
		END
	) = tb1.Grupo
WHERE
	tb2.counterpartyName IS NOT NULL'
EXEC (@cnpjs)
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--										Selecionar tabela com a lsita de Perfil CCEE, os dados mais recentes...
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

	IF OBJECT_ID('tempdb..#ListaPerfil') IS NOT NULL
	BEGIN
		DROP TABLE #ListaPerfil
	END
 
	SELECT * INTO #ListaPerfil
	FROM (
		SELECT 
			CodAgente AS ListaPerfil_CodAgente
			,SiglaAgente As ListaPerfil_SiglaAgente
			,NomeEmpresarial As ListaPerfil_NomeEmpresarial
			,CNPJ As ListaPerfil_CNPJ
			,CodPerfil As ListaPerfil_CodPerfil
			,SiglaPerfil As ListaPerfil_SiglaPerfil
			,ClassePerfil As ListaPerfil_ClassePerfil
			--, CASE WHEN ClassePerfil IS NULL THEN 'Sem Classificação'
			--	ELSE ClassePerfil
			--		END ListaPerfil_ClassePerfil
			,StatusPerfil As ListaPerfil_StatusPerfil
			,CategoriaAgente As ListaPerfil_CategoriaAgente
			,Submercado As ListaPerfil_Submercado
			,PerfilVarejista As ListaPerfil_PerfilVarejista

		-- TO DO: Remover comentário
		--FROM (SELECT * , '2021-01-01' AS DataLista FROM book.DadosIndividuais.VW_ListaPerfil) CC
		--WHERE DataLista = (SELECT MAX(DataLista)  FROM (SELECT * , '2021-01-01' AS DataLista FROM book.DadosIndividuais.ListaPerfil) DD)
		FROM book.DadosIndividuais.VW_ListaPerfil

	) ListaPerfil



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--										Criando uma tabela para aumentar algumas colunas.
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

	IF OBJECT_ID('tempdb..#TempOperation_withColumns') IS NOT NULL
	BEGIN
		DROP TABLE #TempOperation_withColumns
	END
	SELECT * INTO #TempOperation_withColumns FROM(
		SELECT A.* 
			,A.[code] + '-' + A.[sequence] AS codeSequence 
			,ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh])															AS providedVolumeMWh					
		    ,ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])															AS providedVolumeMWm	

		,CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh])	
				  ELSE ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh]) * (100-A.[flexibilityPercentageBottom])/100
				END			 																									AS providedVolumeFlexBottomMWh					
		,CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
				  ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100-A.[flexibilityPercentageBottom])/100
				END			 																									AS providedVolumeFlexBottomMWm				
		,CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh])	
				  ELSE ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh]) * (100+A.[flexibilityPercentageTop])/100
				END			 																									AS providedVolumeFlexTopMWh				
		,CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
				  ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100+A.[flexibilityPercentageTop])/100
				END																												AS providedVolumeFlexTopMWm
		,CASE WHEN (A.[finalVolumeMwm] = ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])) THEN 'Igual'
			  WHEN (A.[finalVolumeMwm] = ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100-A.[flexibilityPercentageBottom])/100) THEN 'Take Minimo'
			  WHEN (A.[finalVolumeMwm] = ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100+A.[flexibilityPercentageTop])/100) THEN 'Take Maximo'
			  WHEN (A.[finalVolumeMwm] < ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])) THEN CASE WHEN (A.[finalVolumeMwm] < CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
																																			  ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100-A.[flexibilityPercentageBottom])/100
																																		 END) THEN 'Recompra'
																											 ELSE 'Limite Take Minimo'
																										END 
			  WHEN (A.[finalVolumeMwm] > ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])) THEN CASE WHEN (A.[finalVolumeMwm] > CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
																																			  ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100+A.[flexibilityPercentageBottom])/100
																																		 END) THEN 'Revenda'
																											 ELSE 'Limite Take Maximo'
																										END 
			  ELSE 'Nao Tratado'
		END																																																																			AS EstadoFlexibilidade_MWm		
		,CASE	WHEN (A.[finalVolumeMwm] = ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])) THEN (A.[finalVolumeMwm] - ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]))
				WHEN (A.[finalVolumeMwm] < ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])) THEN CASE WHEN (A.[finalVolumeMwm] < CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
																																			  ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100-A.[flexibilityPercentageBottom])/100
																																		 END) THEN (CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
																																						  ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100-A.[flexibilityPercentageBottom])/100
																																					 END - ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]))
																										       ELSE (A.[finalVolumeMwm] - ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]))
																										  END 
				WHEN (A.[finalVolumeMwm] > ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])) THEN CASE WHEN (A.[finalVolumeMwm] > CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
																																			  ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100+A.[flexibilityPercentageBottom])/100
																																		 END) THEN (CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
																																						  ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100+A.[flexibilityPercentageBottom])/100
																																					 END - ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]))
																											   ELSE (A.[finalVolumeMwm] - ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]))
																										  END 
				ELSE NULL
		END																																																																			AS FlexibilidadeExercida_MWm	
		,CASE	WHEN (A.[finalVolumeMwh] = ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh])) THEN (A.[finalVolumeMwh] - ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh]))
				WHEN (A.[finalVolumeMwh] < ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh])) THEN CASE WHEN (A.[finalVolumeMwh] < CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh])	
																																			  ELSE ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh]) * (100-A.[flexibilityPercentageBottom])/100
																																		 END) THEN (CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh])	
																																						  ELSE ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh]) * (100-A.[flexibilityPercentageBottom])/100
																																					 END - ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh]))
																										       ELSE (A.[finalVolumeMwh] - ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh]))
																										  END 
				WHEN (A.[finalVolumeMwh] > ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh])) THEN CASE WHEN (A.[finalVolumeMwh] > CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh])	
																																			  ELSE ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh]) * (100+A.[flexibilityPercentageBottom])/100
																																		 END) THEN (CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh])	
																																						  ELSE ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh]) * (100+A.[flexibilityPercentageBottom])/100
																																					 END - ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh]))
																											   ELSE (A.[finalVolumeMwh] - ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh]))
																										  END 
				ELSE NULL
		END																																																																			AS FlexibilidadeExercida_MWh
		,ROUND(CASE WHEN (A.[finalVolumeMwm] = ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])) THEN NULL
					WHEN (A.[finalVolumeMwm] < ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])) THEN CASE WHEN (A.[finalVolumeMwm] < CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
																																			 	   ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100-A.[flexibilityPercentageBottom])/100
																																			  END) THEN ((A.[finalVolumeMwm] - CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
																																											  	    ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100-A.[flexibilityPercentageBottom])/100
																																											   END))
																											       ELSE NULL
																											  END 
					WHEN (A.[finalVolumeMwm] > ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])) THEN CASE WHEN (A.[finalVolumeMwm] > CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
																												  ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100+A.[flexibilityPercentageBottom])/100
																											 END) THEN ((A.[finalVolumeMwm] - CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
																																				  ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100+A.[flexibilityPercentageBottom])/100
																																			 END))
																												   ELSE NULL
																											  END 
					ELSE NULL
				END,6)																																																																AS Renegociacao_MWm	
		FROM #TempOperation AS A
	) as TempOperation_withColumns


	IF OBJECT_ID('tempdb..#TempOperation_withColumnsAndLeftJoins') IS NOT NULL
	BEGIN
		DROP TABLE #TempOperation_withColumnsAndLeftJoins
	END
	SELECT * INTO #TempOperation_withColumnsAndLeftJoins FROM(
		SELECT A.*
			,B.ListaPerfil_CodAgente
			,B.ListaPerfil_SiglaAgente
			,B.ListaPerfil_NomeEmpresarial
			,B.ListaPerfil_CNPJ
			,B.ListaPerfil_CodPerfil
			,B.ListaPerfil_SiglaPerfil
			,CASE WHEN B.ListaPerfil_ClassePerfil IS NULL THEN 'Sem Classificação'
				ELSE B.ListaPerfil_ClassePerfil
					END ListaPerfil_ClassePerfil_Ajustado
			,B.ListaPerfil_StatusPerfil
			,B.ListaPerfil_CategoriaAgente
			,B.ListaPerfil_Submercado
			,B.ListaPerfil_PerfilVarejista

			, (CASE 
			WHEN A.tradeType = 'Compra' THEN 1
			WHEN A.tradeType = 'Venda' THEN -1
		END) * A.contractedVolumeMwh  AS netContractedVolumeMwh 
		, (CASE 
			WHEN A.tradeType = 'Compra' THEN 1
			WHEN A.tradeType = 'Venda' THEN -1
		END) * A.contractedVolumeMwm  AS netContractedVolumeMwm 
			

		FROM #TempOperation_withColumns AS A
		LEFT JOIN #ListaPerfil AS B
		ON A.counterpartyProfileCode = CONVERT(VARCHAR(200),B.ListaPerfil_CodPerfil)
	) as TempOperation_withColumnsAndLeftJoins

--select top 10 * from #TempOperation_withColumnsAndLeftJoins
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--										Criando uma coluna com a área de cada Operador e clssificando curto e longo prazo
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

	IF OBJECT_ID('tempdb..#Final') IS NOT NULL
	BEGIN
		DROP TABLE #Final
	END
	SELECT * INTO #Final FROM(
		SELECT *
		,Book.fn_Resultado(A.basePriceWithReadjustment, FWD_Flex_Preco_Energia, PLD_Preco, A.ceiling, A.floor, FWD_Flex_Preco_PLD, A.Spread, A.tradeType, A.priceTypeDescription, A.FlexibilidadeExercida_MWh)	as ResultadoFlexProvidedVolume
		,Book.fn_Resultado(A.baseprice, FWD_createdAt_Preco_Energia, PLD_Preco, A.ceiling, A.floor, FWD_createdAt_Preco_PLD, A.Spread, A.tradeType, A.priceTypeDescription, A.ContractedVolumeMwh)	as ResultadoSpreadNovo
		,Book.fn_Resultado(A.price, FWD_Preco_Energia, PLD_Preco, A.ceiling, A.floor, FWD_Preco_PLD, A.Spread, A.tradeType, A.priceTypeDescription, A.providedVolumeMWh)	as ResultadoProvidedVolume
		,Book.fn_Resultado(A.price, FWD_Preco_Energia, PLD_Preco, A.ceiling, A.floor, FWD_Preco_PLD, A.Spread, A.tradeType, A.priceTypeDescription, A.providedVolumeFlexBottomMWh)	as ResultadoProvidedVolumeFlexBottom
		,Book.fn_Resultado(A.price, FWD_Preco_Energia, PLD_Preco, A.ceiling, A.floor, FWD_Preco_PLD, A.Spread, A.tradeType, A.priceTypeDescription, A.providedVolumeFlexTopMWh)	as ResultadoProvidedVolumeFlexTop
		, (CASE 
			WHEN A.tradeType = 'Compra' THEN 1
			WHEN A.tradeType = 'Venda' THEN -1
		END) * A.finalVolumeMwh  AS netFinalVolumeMwh
		, (CASE 
			WHEN A.tradeType = 'Compra' THEN 1
			WHEN A.tradeType = 'Venda' THEN -1
		END) * A.finalVolumeMwm  AS netFinalVolumeMwm
		, CASE
			WHEN A.Thunders = 'Safira' or A.Thunders = 'Comercial' THEN
				(CASE 
					WHEN A.userOperatorName IN ('Pedro Comandini','Francisco Galdino','Julia Zanzarini Lopes','Gabriel Eugênio','Rodrigo Morais','Leilane Santos','Rodrigo Morais','Romulo Bellotto','Frederico de Oliveira Cavalcante') THEN 'Comercial'
					WHEN A.userOperatorName IN ('Elias Skaf','Henrique Bergamo','Kevin Lima', 'Denis Fischer') THEN 'Mesa' Else 'Outros' END)
			
			WHEN A.Thunders = 'Indra' THEN
				(CASE 
					WHEN A.userOperatorName IN ('Thiago Veiga','Ingrid','Nicolas Villa','Romulo Bellotto', 'Marcio Queiroz Davanzo','Márcio') THEN 'Indra' Else 'Outros' END)

		END as Area
		, CASE
			WHEN A.Thunders = 'Safira' THEN
				(CASE
					WHEN A.classifications in ('COMERCIAL MESA','CORPORATE','ESTRATEGIA MESA','GIRO RAPIDO','SPOT MESA','VAREJO') THEN 'Mesa'
					WHEN A.classifications in ('INDRA','OPERAÇÕES_ESTRUTURADAS','Derivativo','DIRECIONAL','FIN_GR','SWAP SAFIRA','SPOT SAFIRA') THEN 'Mikio'
					WHEN A.classifications in ('Fuga','OP PF') THEN A.classifications
					WHEN A.classifications in ('CORPORATE_MESA','MANAUS_MESA','VAREJO_MESA') THEN 'Comercial_Proprio'
					ELSE 'Sem Classificação' END)
			WHEN A.Thunders = 'Comercial' THEN	
				(CASE
					WHEN A.classifications in ('Mesa2') THEN 'Mesa'
					WHEN A.classifications in ('Sfr2') THEN 'Mikio'
					WHEN A.classifications in ('Safira') THEN 'Comercial_Proprio'
					WHEN A.classifications in ('Próprio','','LP_proprio','CP_proprio') THEN 'Comercial_Proprio'
					WHEN A.classifications in ('Indra') THEN 'Comercial_Indra'
					WHEN A.classifications in ('Varejista') THEN 'Comercial_Varejista'
					ELSE 'Sem Classificação' END)
			WHEN A.Thunders = 'Indra' THEN
				(CASE
					WHEN A.classifications in ('MoM2') THEN 'Indra'
					ELSE 'Indra' END) END AS classificationsRename
--		, CASE
--			WHEN (DATEDIFF(MONTH, A.startDate, A.enddate) + 1 > 1
--			) THEN 'Longo Prazo'
--			
--			WHEN (
--				DATEDIFF(MONTH, A.startDate, A.enddate) + 1 = 1 
--				AND A.priceTypeDescription = 'Variável'
--			) THEN 'Curto Prazo'
--
--			--novo curto prazo abaixo
--			WHEN (
--				DATEDIFF(MONTH, A.DataCriacao, A.DataFornecimento) = 1 
--				OR  MONTH(A.startDate) = MONTH(A.endDate) 
--					AND YEAR(A.startDate) = YEAR(A.endDate)
--			) THEN 'Curto Prazo'
--
--			WHEN (
--				DATEDIFF(MONTH, A.startDate, A.enddate) + 1 = 1 
--				AND (
--					MONTH(A.startDate) = MONTH(A.DataCriacao) 
--					AND YEAR(A.startDate) = YEAR(A.DataCriacao)
--				)
--			) THEN 'Curto Prazo'
--			
--			WHEN (
--				DATEDIFF(MONTH, A.startDate, A.enddate) = 1
--			) THEN 'Possivelmente Curto Prazo'
--			
--			ELSE 'Não classificado'
--			END typePrazo

		,CASE
			WHEN (DATEDIFF(MONTH, A.startDate, A.enddate) + 1 > 1
			) THEN 'Longo Prazo'

			--Acrescentei isso aqui em baixo, acrdito que resolve o problema, teriamos que verificar a dferença de uma regra para a outra para ver se não caga tudo
			WHEN (
				--DATEDIFF(MONTH, A.DataCriacao, A.startDate) +1 > 1 -- e colocar o +1 ele vai acabar pegando quando os caras fazem boleta para o mês subsequente, não sei se seria certo....
				DATEDIFF(MONTH, A.DataCriacao, A.startDate)> 1 
			) THEN 'Longo Prazo'

			--novo curto prazo abaixo
			WHEN (
				DATEDIFF(MONTH, A.DataCriacao, A.DataFornecimento) = 1 
				-- Esta condição esta errada, a boleta da liasa tem START DATE AND AND DATE iguais... no segundo OR deveria ter um AND "SE Start Date = END DATE E Data criação  < data fornecimento"
				OR  ( MONTH(A.startDate) = MONTH(A.endDate) 
					AND YEAR(A.startDate) = YEAR(A.endDate)
				)
			) THEN 'Curto Prazo'

			WHEN (
				DATEDIFF(MONTH, A.startDate, A.enddate) + 1 = 1 
				AND (
					MONTH(A.startDate) = MONTH(A.DataCriacao) 
					AND YEAR(A.startDate) = YEAR(A.DataCriacao)
				)
			) THEN 'Curto Prazo'
			
			WHEN (
				DATEDIFF(MONTH, A.startDate, A.enddate) = 1
			) THEN 'Possivelmente Curto Prazo'
			
			ELSE 'Não classificado'
			END typePrazo

		,CASE
			WHEN A.contractedVolumeMwm < 1 THEN 'F (Vol. MWm Contratado < 1)' 
			WHEN A.contractedVolumeMwm BETWEEN 1 AND 2 THEN 'E (Vol. MWm Entre 1 e 2)'
			WHEN A.contractedVolumeMwm BETWEEN 2 AND 3 THEN 'D (Vol. MWm Entre 2 e 3)'
			WHEN A.contractedVolumeMwm BETWEEN 3 AND 4 THEN 'C (Vol. MWm Entre 3 e 4)'
			WHEN A.contractedVolumeMwm BETWEEN 4 AND 5 THEN 'B (Vol. MWm Entre 4 e 5)'
			WHEN A.contractedVolumeMwm > 5 THEN 'A (Vol. MWm Contratado > 5)'
			ELSE 'Não classificado'
			END contractedVolumeClientClassification
		
		,sum(contractedVolumeMwh*basePrice) OVER( PARTITION BY year,Code,thunders,sequence,businessUnitDescription ORDER BY Code) / NULLIF(sum(contractedVolumeMwh) OVER( PARTITION BY year,Code,thunders,sequence,businessUnitDescription ORDER BY Code),0) media_price
		,sum(contractedVolumeMwh*FWD_createdAt_Preco_Energia) OVER( PARTITION BY year,Code,thunders,sequence,businessUnitDescription ORDER BY Code) / NULLIF(sum(contractedVolumeMwh) OVER( PARTITION BY year,Code,thunders,sequence,businessUnitDescription ORDER BY Code),0) media_fwd

		--,avg(FWD_createdAt_Preco_Energia) OVER( PARTITION BY year,Code ORDER BY Code) media_fwd	
		
		FROM #TempOperation_withColumnsAndLeftJoins AS A) as Final

--SELECT * FROM #Final WHERE YEAR=2022 AND MONTH=12 AND code = 'VC306-22'
--SELECT TOP 10 * FROM #Final

---colocar outras contas aqui

----------------------------------------------------------------------------------------------------------------------------
--									TABELA TEMPORARIA PARA CALCULO RESULTADO
----------------------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#nome1') IS NOT NULL
	BEGIN
		DROP TABLE #nome1
	END
	SELECT * INTO #nome1 FROM(
		SELECT *
		,(CASE WHEN FWD_createdAt_Preco_Energia is null THEN 0 ELSE 1 END) * 
			(CASE WHEN tradeType = 'Compra' THEN ((CASE WHEN media_fwd-media_price < 0 THEN 1 
															ELSE media_fwd-media_price 
															END))
				 WHEN tradeType = 'Venda' THEN ((CASE WHEN media_price-media_fwd < 0 THEN 1 
															ELSE media_price-media_fwd 
															END))
				END) AS spreadnovo2_media
		,(CASE WHEN FWD_createdAt_Preco_Energia is null THEN 0 ELSE 1 END) *
			(CASE WHEN tradeType = 'Compra' THEN ((CASE WHEN media_fwd-media_price < 0 THEN 1 
														 ELSE media_fwd-media_price
													    END))
			   WHEN tradeType = 'Venda' THEN ((CASE WHEN media_price-media_fwd < 0 THEN 1 
														 ELSE media_price-media_fwd
														 END))
				END) *contractedVolumeMwh as resultadocomercial2
		,CASE WHEN tradeType = 'Compra' THEN ((CASE WHEN media_fwd-media_price < 0 THEN 'Esforço' 
														ELSE 'Resultado' 
													END))
			  WHEN tradeType = 'Venda' THEN ((CASE WHEN media_price-media_fwd < 0 THEN 'Esforço' 
														ELSE 'Resultado' 
													END))
		       END AS medida_resultado_comercial
		,(CASE WHEN FWD_createdAt_Preco_Energia is null THEN 0 ELSE 1 END) * 
			(CASE WHEN tradeType = 'Compra' THEN ((CASE WHEN FWD_createdAt_Preco_Energia-basePrice < 0 THEN 1 
															ELSE FWD_createdAt_Preco_Energia-basePrice 
															END))
				 WHEN tradeType = 'Venda' THEN ((CASE WHEN basePrice-FWD_createdAt_Preco_Energia < 0 THEN 1 
															ELSE basePrice-FWD_createdAt_Preco_Energia 
															END))
				END) AS spreadnovo3_
	FROM #Final
			) AS #nome1


--IF OBJECT_ID('tempdb..#test1') IS NOT NULL
--	BEGIN
--		DROP TABLE #test1
--	END
--	SELECT * INTO #test1 FROM(
--		SELECT *,
--		sum(finalVolumeMwh*spreadnovo2) OVER( PARTITION BY year,Code ORDER BY Code) / NULLIF(sum(finalVolumeMwh) OVER( PARTITION BY year,Code ORDER BY Code),0) media_spreadnovo2
--	FROM #nome1
--			) AS #test1

---------------------------------------------------------------------------------------------------------------------------
--                                TABELA DE ALTERAÇÃO MANUAL DE BOLETA
---------------------------------------------------------------------------------------------------------------------------
-- ESSA PARTE DO CÓDIGO FOI FEITA DEVIDO A UMA NEGOCIAÇÃO DA INGREDION
-- BOLETA ANTIGA ZERADA
-- BOLETA NOVA DUPLICANDO RESULTADO
-- BOLETA NOVA VC068-23


-- criacao coluna SWAP - Bike pediu pra retirar os valores de swap do relatorio

IF OBJECT_ID('tempdb..#alteracao') IS NOT NULL
BEGIN
	DROP TABLE #alteracao
END
	SELECT * INTO #alteracao FROM(
	SELECT * 
		, CASE WHEN code in ('VC068-23', -- BOLETA INGREDION QUE FOI CRIADA 
							 'VC003-21' -- BOLETA INGREDION QUE FOI ZERADA
								) THEN 'Sim' ELSE 'Não' END AS Alteracao_Manual_Esforco_E_Resultado
		, CASE WHEN code in ('VI5139-23',
							 'VI5140-23',
							 'VI5138-23') THEN 'Sim' ELSE 'Não' END AS SWAP
		 FROM #nome1 ) #alteracao


IF OBJECT_ID('tempdb..#alteracao2') IS NOT NULL
BEGIN
	DROP TABLE #alteracao2
END
	SELECT * INTO #alteracao2 FROM(
	SELECT * 
	, Case When Alteracao_Manual_Esforco_E_Resultado = 'Sim' Then 0 Else ResultadoSpreadNovo END AS ResultadoSpreadNovoCorrigido
	, Case When Alteracao_Manual_Esforco_E_Resultado = 'Sim' Then 0 Else resultadocomercial2 END AS resultadocomercial2Corrigido
		FROM #alteracao) alteracao2


IF OBJECT_ID('tempdb..#alteracao3') IS NOT NULL
BEGIN
	DROP TABLE #alteracao3
END
	SELECT * INTO #alteracao3 FROM(
	SELECT * 
	,CASE WHEN Alteracao_Manual_Esforco_E_Resultado = 'SIM' and code = 'VC068-23' and MONTH(DataFornecimento) = 08 and YEAR(DataFornecimento) = 2023 and sequence = 1 then  '1542212.60'  
		  WHEN Alteracao_Manual_Esforco_E_Resultado = 'SIM' and code = 'VC003-21' and MONTH(DataFornecimento) = 01 and YEAR(DataFornecimento) = 2023 and sequence = 1 then '-30660.00' 
	else ResultadoSpreadNovoCorrigido end as ResultadoSpreadNovoCorrigidov2
	,CASE WHEN Alteracao_Manual_Esforco_E_Resultado = 'SIM' and code = 'VC068-23' and MONTH(DataFornecimento) = 08 and YEAR(DataFornecimento) = 2023 and sequence = 1 then '1542212.60'
		  WHEN Alteracao_Manual_Esforco_E_Resultado = 'SIM' and code = 'VC003-21' and MONTH(DataFornecimento) = 01 and YEAR(DataFornecimento) = 2023 and sequence = 1 then '-30660.00'
	else resultadocomercial2Corrigido end as resultadocomercial2Corrigidov2
	FROM #alteracao2) #alteracao3

----------------------------------------------------------------------------------------------------------------------------
--									TABELA TEMPORARIA PARA O SELECT FINAL
----------------------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#GabrielPires') IS NOT NULL
	BEGIN
		DROP TABLE #GabrielPires
	END
	SELECT * INTO #GabrielPires FROM(
		SELECT * 
		, CASE WHEN ResultadoProvidedVolume  = Resultado THEN 'Sim'
			ELSE 'Não' 
				END AS Validar
		, CONCAT(code, '-', Thunders) AS distinctCode
		, CASE WHEN finalVolumeMwh = '0'
			THEN 'Boleta zerada'
				ELSE 'Boleta não zerada'
					END BoletaZerada
		, CASE WHEN code = 'VI5013-24' THEN 'Varejista'
			   WHEN code = 'VI5009-25' THEN 'Economia Garantida' 
			   ELSE 'Agente'
			END AS Tipo_Cliente
		, CASE WHEN code = 'VI5013-24' THEN 'Modalidade Varejista. Comissão Clark R$18,00/MWh' 
			   WHEN code = 'VI5009-25' THEN '21% de Economia Garantida para 2025 e 2026'
			   ELSE 'Sem Observação' 
			END AS ObsCliente
		, CASE WHEN code = 'VI5013-24' THEN 18 ELSE 0 END AS Comissao
		, @load_datetime AS load_datetime
	FROM #alteracao3
	--WHERE year >= '2021'
	WHERE Thunders <> 'Indra') AS #GabrielPires

	---colocar resultado aqui

----------------------------------------------------------------------------------------------------------------------------
--														 TABELA PREJUIZO LUCRO
----------------------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#GroupBy') IS NOT NULL
BEGIN
	DROP TABLE #GroupBy
END
	SELECT * INTO #GroupBy FROM(
		SELECT	SUM(NetNotaFiscal) AS NotaFiscalFinal
				, counterpartyname 
				, CASE WHEN SUM(NetNotaFiscal) >= 0 THEN 'Ativo'
					ELSE 'Passivo'
						END AS 'Classificacao'
				FROM #GabrielPires
		WHERE BoletaZerada = 'Boleta não zerada'
		AND TypeBusiness = 'Negocio Externo'
		AND isActive = 'True'
		GROUP BY counterpartyname) AS #GroupBy

--Salvar #temp em uma tabela final
IF OBJECT_ID('Book.Book.proc_InformacaoComercial_table0') IS NOT NULL
BEGIN
	DROP TABLE Book.Book.proc_InformacaoComercial_table0
END
SELECT * INTO Book.Book.proc_InformacaoComercial_table0 FROM #GroupBy

----------------------------------------------------------------------------------------------------------------------------
--														 TABELA BLACK LIST
----------------------------------------------------------------------------------------------------------------------------
--- Criando tabela #BlackList
IF OBJECT_ID('tempdb..#BlackList') IS NOT NULL
	BEGIN
		DROP TABLE #BlackList 
	END

CREATE TABLE #BlackList (
		counterpartyName NVARCHAR(250) NULL
		, counterpartyCNPJ NVARCHAR(20) NOT NULL
		, DataInsercao DATETIME2 NOT NULL
		, Motivo NVARCHAR(250) NOT NULL
		)

INSERT INTO #BlackList (counterpartyName, counterpartyCNPJ, DataInsercao, Motivo)
	VALUES	('SICBRAS',			'00000000000000', '2022-06-29', 'Qualquer')
			,('NEMAK',			'00000000000000', '2022-06-29', 'Qualquer')
			,('RIMO',			'00000000000000', '2022-06-29', 'Qualquer')
			,('FRAPORT',		'00000000000000', '2022-06-29', 'Qualquer')
			,('ARO',			'00000000000000', '2022-06-29', 'Qualquer')
			,('PRISMATIC',		'00000000000000', '2022-06-29', 'Qualquer')
			,('DALILA TÊXTIL',	'00000000000000', '2022-06-29', 'Qualquer')
			,('PASSAMANARIA',	'00000000000000', '2022-06-29', 'Qualquer')
			,('LAGHETTO',		'00000000000000', '2022-06-29', 'Qualquer')
			,('JOY ALIMENTOS',	'00000000000000', '2022-06-29', 'Qualquer')
			,('LH BARRA RIO',	'00000000000000', '2022-06-29', 'Qualquer')
			,('ELDOR',			'00000000000000', '2022-06-29', 'Qualquer')

--Salvar #temp em uma tabela final
IF OBJECT_ID('Book.Book.proc_InformacaoComercial_table1') IS NOT NULL
	BEGIN
		DROP TABLE Book.Book.proc_InformacaoComercial_table1
	END
SELECT * INTO Book.Book.proc_InformacaoComercial_table1 FROM #BlackList
----------------------------------------------------------------------------------------------------------------------------
--														 TABELA METAS COMERCIAL
----------------------------------------------------------------------------------------------------------------------------
-- Criando tabela #metaComercial
IF OBJECT_ID('tempdb..#MetaComercial') IS NOT NULL
	BEGIN
		DROP TABLE #MetaComercial 
	END
		SELECT * INTO #MetaComercial FROM(
	SELECT * FROM [TREINAMENTO].[tcasado].[meta_comercial]
	) MetaComercial
-- select * from #MetaComercial

-- Salvar #temp em uma tabela final
IF OBJECT_ID('Book.Book.proc_InformacaoComercial_table2') IS NOT NULL
	BEGIN
		DROP TABLE Book.Book.proc_InformacaoComercial_table2
	END
SELECT 
		  DataInsert
		 ,DataCriacao
		 ,AnoCriacao
		 ,DataFornecimento
		 ,Meta
		 --,(Meta/12) AS Meta
		 ,Spread
		INTO Book.Book.proc_InformacaoComercial_table2 FROM #MetaComercial   
-- select * from #MetaComercial
----------------------------------------------------------------------------------------------------------------------------
--														 TABELA OPERADORES
----------------------------------------------------------------------------------------------------------------------------
--- Criando tabela #Operador
IF OBJECT_ID('tempdb..#Operador') IS NOT NULL
	BEGIN
		DROP TABLE #Operador
	END

CREATE TABLE #Operador (
		DataInsert DATETIME2 NOT NULL
		,AnoCriacao varchar (50) NOT NULL
		,Operador NVARCHAR(50) NULL
		,Peso float
		)

INSERT INTO #Operador (DataInsert,AnoCriacao,Operador,Peso)
--DataInsert (refere-se a meta)
--MesInsert (refere-se aos pesos)
	VALUES	 ('2022-06-30',	'2022',  'Julia Zanzarini Lopes'			,'0.15')
			,('2022-06-30',	'2022',  'Gabriel Eugênio'					,'0.10')
			,('2022-06-30',	'2022',  'Pedro Comandini'					,'0.15')
			,('2022-06-30',	'2022',  'Romulo Bellotto'					,'0.21')
			,('2022-06-30',	'2022',  'Francisco Galdino'				,'0.21')
			,('2022-06-30',	'2022',  'Leilane Santos'					,'0.18')
			,('2022-06-30',	'2023',  'Julia Zanzarini Lopes'			,'0.25')
			,('2022-06-30',	'2023',  'Gabriel Eugênio'					,'0.15')
			,('2022-06-30',	'2023',  'Pedro Comandini'					,'0.25')
			,('2022-06-30',	'2023',  'Leilane Santos'					,'0.25')
			,('2022-06-30',	'2023',  'Frederico de Oliveira Cavalcante'	,'0.10')
			,('2023-07-01',	'2023',  'Julia Zanzarini Lopes'			,'0.25')
			,('2023-07-01',	'2023',  'Gabriel Eugênio'					,'0.15')
			,('2023-07-01',	'2023',  'Pedro Comandini'					,'0.25')
			,('2023-07-01',	'2023',  'Leilane Santos'					,'0.25')
			,('2023-07-01',	'2023',  'Frederico de Oliveira Cavalcante'	,'0.10')


ALTER TABLE #Operador
  ALTER COLUMN AnoCriacao
    VARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL

--USE TREINAMENTO;
--go
--EXEC sp_help 'tcasado.meta_comercial';
--go
--USE Book;
--go
--EXEC sp_help 'Book.proc_InformacaoComercial_table3';
--go

--Salvar #temp em uma tabela final
IF OBJECT_ID('Book.Book.proc_InformacaoComercial_table3') IS NOT NULL
	BEGIN
		DROP TABLE Book.Book.proc_InformacaoComercial_table3
	END
SELECT * INTO Book.Book.proc_InformacaoComercial_table3 FROM #Operador
--select distinct operador from Book.Book.proc_InformacaoComercial_table3

----------------------------------------------------------------------------------------------------------------------------
--														 TABELA FINAL META COMERCIAL
----------------------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#MetaFinal') IS NOT NULL
BEGIN
	DROP TABLE #MetaFinal
END
	SELECT * INTO #MetaFinal FROM(
		select 
			a.DataInsert,
			a.DataCriacao,
			a.DataFornecimento,
			a.Spread,
			--a.Meta,
			b.Operador,
			b.Peso,
			c.typeaa,
			(a.Meta * b.Peso) as Meta
				from Book.Book.proc_InformacaoComercial_table2  as a
				LEFT JOIN Book.Book.proc_InformacaoComercial_table3 as b
				ON a.AnoCriacao COLLATE DATABASE_DEFAULT  = b.AnoCriacao COLLATE DATABASE_DEFAULT
				and a.DataInsert = b.DataInsert
				cross join (
						select 'Compra' as typeaa
						union all
						select 'Venda' as typeaa) C
				) AS #MetaFinal
				-- select * from #MetaFinal

--Salvar #temp em uma tabela final
IF OBJECT_ID('Book.Book.proc_InformacaoComercial_table4') IS NOT NULL
BEGIN
	DROP TABLE Book.Book.proc_InformacaoComercial_table4
END
SELECT * INTO Book.Book.proc_InformacaoComercial_table4 FROM #MetaFinal

--select distinct operador from Book.Book.proc_InformacaoComercial_table4
----------------------------------------------------------------------------------------------------------------------------
--														 SELECT FINAL
----------------------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('Book.Book.proc_InformacaoComercial_table5') IS NOT NULL
BEGIN
	DROP TABLE Book.Book.proc_InformacaoComercial_table5
END
SELECT * INTO Book.Book.proc_InformacaoComercial_table5 FROM #GabrielPires

--Select * from #GabrielPires

----------------------------------------------------------------------------------------------------------------------------
--                                                    SELECT FINAL COM CNAE
----------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS #GabrielPires2;
SELECT * INTO #GabrielPires2 FROM (
	SELECT
	*,
	CNPJ_X = ISNULL(ListaPerfil_CNPJ,RIGHT('00000000000000'+REPLACE(REPLACE(REPLACE(counterpartyCNPJ,'.',''),'-',''),'/',''),14))
	FROM #GabrielPires
) GabrielPires2


DROP TABLE IF EXISTS #CNAE;
SELECT * INTO #CNAE FROM (
	SELECT
		G.*,
		e.cnae_fiscal_principal
		,e.uf 
	FROM #GabrielPires2 G
	LEFT JOIN
		MOdelo.ReceitaFederal.estabelecimentos E
	ON G.CNPJ_X = E.cnpj
) CNAE


DROP TABLE IF EXISTS #SELECT_COMPLETO;
SELECT * INTO #SELECT_COMPLETO FROM (
SELECT 
	EST.*,
	CNAE.cnae_Denominacao_Secao,
	CNAE.cnae_Denominacao_Divisao
FROM #CNAE AS EST
LEFT JOIN Book.IBGE.VW_cnae AS CNAE
ON
    EST.cnae_fiscal_principal = REPLACE(REPLACE(REPLACE(CNAE.cnae_SubClasse,'-',''),'.',''),'/', '')
) SELECT_COMPLETO



IF OBJECT_ID('Book.Book.proc_InformacaoComercial_table6') IS NOT NULL
BEGIN
	DROP TABLE Book.Book.proc_InformacaoComercial_table6
END
SELECT * INTO Book.Book.proc_InformacaoComercial_table6 FROM #SELECT_COMPLETO

select * from #SELECT_COMPLETO



   --select * from Book.IBGE.VW_cnae

	--startDate >= '2021-01-01T00:00:00'
	--AND Thunders <> 'Indra'
	--AND finalVolumeMwh <> 0 
	--AND isActive = 1

--- Dando permissão no select
GRANT SELECT ON [Book].[proc_InformacaoComercial_table5] TO [SAFIRA\hbergamo]
GRANT SELECT ON [Book].[proc_InformacaoComercial_table5] TO [SAFIRA\klima]
GRANT SELECT ON [Book].[proc_InformacaoComercial_table5] TO [SAFIRA\jlopes]
GRANT SELECT ON [Book].[proc_InformacaoComercial_table5] TO [SAFIRA\fgaldino]
 
--15/08
GRANT SELECT ON [Book].[proc_InformacaoComercial_table5] TO [SAFIRA\gbarreto]
GRANT SELECT ON [Book].[proc_InformacaoComercial_table5] TO [SAFIRA\dfischer]
--28/04
GRANT SELECT ON [Book].[proc_InformacaoComercial_table6] TO [SAFIRA\hbergamo]

--16/05/2024
GRANT SELECT ON [Book].[proc_InformacaoComercial_table5] TO [SAFIRA\gustavo.mendonca]
GRANT SELECT ON [Book].[proc_InformacaoComercial_table6] TO [SAFIRA\gustavo.mendonca]

GRANT SELECT ON [Book].[proc_InformacaoComercial_table5] TO [SAFIRA\joao.castro]
GRANT SELECT ON [Book].[proc_InformacaoComercial_table6] TO [SAFIRA\joao.castro]

GRANT SELECT ON [Book].[proc_InformacaoComercial_table5] TO [SAFIRA\fsilva]
GRANT SELECT ON [Book].[proc_InformacaoComercial_table6] TO [SAFIRA\fsilva]

GRANT SELECT ON [Book].[proc_InformacaoComercial_table5] TO [SAFIRA\tikuta]
GRANT SELECT ON [Book].[proc_InformacaoComercial_table6] TO [SAFIRA\tikuta]

--16-09-2024
GRANT SELECT ON [Book].[proc_InformacaoComercial_table5] TO [SAFIRA\jonathas.olsen]
GRANT SELECT ON [Book].[proc_InformacaoComercial_table6] TO [SAFIRA\jonathas.olsen]




GO


