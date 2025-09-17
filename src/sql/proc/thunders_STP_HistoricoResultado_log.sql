USE [Book]
GO

/****** Object:  StoredProcedure [Book].[STP_HistoricoResultado_log]    Script Date: 28/02/2025 08:36:28 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




ALTER PROCEDURE [Book].[STP_HistoricoResultado_log]
-------------------------------------------------------------------------------------------------------------------------------------------
												-- Declaração das variaveis--
-------------------------------------------------------------------------------------------------------------------------------------------
@DataInicio DATE
,@DataFim	DATE
,@Curva     VARCHAR(250)

AS
BEGIN

-------------------------------------------------------------------------------------------------------------------------------------------
						-- Selecionar as datas da Curva Fwd OFICIAL que estão entre as datas de inicio e fim--
-------------------------------------------------------------------------------------------------------------------------------------------
	IF OBJECT_ID('tempdb..#TempDatas') IS NOT NULL
	BEGIN
	   DROP TABLE #TempDatas
	END

	SELECT * INTO #TempDatas
	FROM (
			SELECT 
				A.*
				, CONVERT(NVARCHAR(MAX), A.[Data] )  + ' 23:59:59.000' AS Datahora 
			FROM (
				SELECT DISTINCT [Data] 
				FROM book.[Curva].[Curva_Fwd] 
				WHERE 
					Curva = @Curva 
					AND [Data] BETWEEN @DataInicio AND @DataFim
			) A
		) Tempdatas


-------------------------------------------------------------------------------------------------------------------------------------------
													    -- Setar as variaveis para fazer o LOOP entre datas --
														-- @StartDate - Data Minima da tabela #TempDatas
														-- @DataFim - FData Maxima da tabela #TempDatas
-------------------------------------------------------------------------------------------------------------------------------------------

DECLARE @StartDate AS DATETIME
DECLARE @EndDate AS DATETIME
DECLARE @CurrentDate AS DATETIME

-------------------------------------------------------------------------------------------------------------------------------------------
													-- Criar a Tabela final para ser inserido  --
-------------------------------------------------------------------------------------------------------------------------------------------

IF OBJECT_ID('tempdb..#MyTable') IS NOT NULL
BEGIN
   DROP TABLE #MyTable
END
CREATE TABLE #MyTable (	DataHistorico				DATETIME
						, DataFornecimento			DATE
						, NaturezaOperacao    		[VARCHAR](250)
						, FonteEnergia				[VARCHAR](250)
						, Submercado				[VARCHAR](250)
						, Thunders					[VARCHAR](50)
						, Classificacao				[VARCHAR](250)
						, FlexibilidadePreco		[VARCHAR](250)
						, TipoFlexibilidadePreco	[VARCHAR](250)
						, VolumeFinal_MWh			FLOAT
						, VolumeFinal_MWm			FLOAT
						, PrecoContrato				FLOAT
						, PrecoFinal				FLOAT
						, Spread					FLOAT
						, TetoPreco					FLOAT
						, PisoPreco					FLOAT
						, Preco						FLOAT
						, Curva						[VARCHAR](50)
						, Preco_PLD					FLOAT
						, Preco_Energia				FLOAT
						, Resultado 				FLOAT);


-------------------------------------------------------------------------------------------------------------------------------------------
													-- Setar as Variaveis StartDate and End Date --
-------------------------------------------------------------------------------------------------------------------------------------------

SET @StartDate = (SELECT MIN([Data]) AS DATE FROM #TempDatas)
	SET @EndDate = (SELECT MAX([Data]) AS DATE FROM #TempDatas)
	SET @CurrentDate = @StartDate

	WHILE (@CurrentDate <= @EndDate)
	BEGIN
		IF EXISTS (
			SELECT 1 
			FROM #TempDatas 
			WHERE [Data] = @CurrentDate
		)
		BEGIN
		-------------------------------------------------------------------------------------------------------------------------------------------
															-- Selecionar a curva FWD --
		-------------------------------------------------------------------------------------------------------------------------------------------
		IF OBJECT_ID('tempdb..#TempCurvaFwd') IS NOT NULL
			BEGIN
			   DROP TABLE #TempCurvaFwd
			END

			SELECT * INTO #TempCurvaFwd
			FROM (
					SELECT * 
					FROM book.curva.[VW_Curva_Fwd] 
					WHERE 
						[Data] = @CurrentDate 
						AND curva = @Curva
				) Tempdatas

		-------------------------------------------------------------------------------------------------------------------------------------------
											-- Tratamento da Tabela Mãe
											-- Remoção das Colunas que não serão Utilizadas
											-- Remoção das boletas de trading
		-------------------------------------------------------------------------------------------------------------------------------------------
		IF OBJECT_ID('tempdb..#TempMae') IS NOT NULL
		BEGIN
		   DROP TABLE #TempMae
		END

		SELECT * INTO #TempMae
		FROM (
				SELECT 
					DataHistorico
					, DataFornecimento
					, NaturezaOperacao
					, FonteEnergia
					, Submercado
					, Thunders
					, Classificacao
					, FlexibilidadePreco
					, TipoFlexibilidadePreco
					, VolumeFinal_MWh
					, VolumeFinal_MWm
					, PrecoContrato
					, PrecoFinal
					, Spread
					, TetoPreco
					, PisoPreco
				FROM book.Book.HistoricoPosicao_log 
				WHERE datahistorico = @CurrentDate + ' 23:59:59.000'
				-- Antigamente era istradind = 1, a partir de 24-11-2023, é preciso alterar para isservices !=1
				and isServices != 1
			) TempMae

		-------------------------------------------------------------------------------------------------------------------------------------------
							-- Select para selecionar preço médio das boletas de preço fixo e preço variável
								-- Note que temos boletas em OPCAO, é preciso seleciona-las também, 
		-------------------------------------------------------------------------------------------------------------------------------------------
		IF OBJECT_ID('tempdb..#TempMaeReduzida') IS NOT NULL
		BEGIN
		   DROP TABLE #TempMaeReduzida
		END
		-- Tabela com os preços Médios e com as separações de Fixo/Variavel/Collar/Opção
		SELECT* INTO #TempMaeReduzida
		FROM (
			-- Esse select só pega Fixo/Variavel
			SELECT
				  DataHistorico
				, DataFornecimento
				, NaturezaOperacao
				, FonteEnergia
				, Submercado
				, Thunders
				, Classificacao
				, FlexibilidadePreco
				, TipoFlexibilidadePreco
				, SUM(VolumeFinal_MWh) AS VolumeFinal_MWh
				, SUM(VolumeFinal_MWm) AS VolumeFinal_MWm
				, SUM(VolumeFinal_MWh * PrecoContrato) / NULLIF( SUM(VolumeFinal_MWh),0) AS PrecoContrato
				, SUM(VolumeFinal_MWh * PrecoFinal) / NULLIF( SUM(VolumeFinal_MWh),0) AS PrecoFinal
				, SUM(VolumeFinal_MWh * Spread) / NULLIF( SUM(VolumeFinal_MWh),0) AS Spread
				, SUM(TetoPreco) AS TetoPreco
				, SUM(PisoPreco) AS PisoPreco
			FROM #TempMae
			WHERE TipoFlexibilidadePreco  IN ('Fixo','Variável')
			GROUP BY
				  DataHistorico
				, DataFornecimento
				, NaturezaOperacao
				, FonteEnergia
				, Submercado
				, Thunders
				, Classificacao
				, FlexibilidadePreco
				, TipoFlexibilidadePreco

			UNION ALL

			-- Esse select considera apenas o COLLAR e a OPCAO, serve para ajudar no VaR, já que será por contrato.
			SELECT
				  DataHistorico
				, DataFornecimento
				, NaturezaOperacao
				, FonteEnergia
				, Submercado
				, Thunders
				, Classificacao
				, FlexibilidadePreco
				, TipoFlexibilidadePreco
				, VolumeFinal_MWh AS VolumeFinal_MWh
				, VolumeFinal_MWm AS VolumeFinal_MWm
				, PrecoContrato AS PrecoContrato
				, PrecoFinal  AS PrecoFinal
				, Spread AS Spread
				, TetoPreco
				, PisoPreco
			FROM #TempMae
			WHERE TipoFlexibilidadePreco IN ('Collar','Opção')

			) TempMaeReduzida

		-------------------------------------------------------------------------------------------------------------------------------------------
							-- Select para fazer os Left Joins
		-------------------------------------------------------------------------------------------------------------------------------------------
			IF OBJECT_ID('tempdb..#TempLeftJoinCurvaResultado') IS NOT NULL
			BEGIN
			   DROP TABLE #TempLeftJoinCurvaResultado
			END

			SELECT * INTO #TempLeftJoinCurvaResultado
			FROM (
				SELECT	  
					A.datahistorico
					, A.dataFornecimento
					, A.NaturezaOperacao
					, A.FonteEnergia
					, A.Submercado
					, A.Thunders
					, A.Classificacao
					, A.flexibilidadePreco
					, A.TipoFlexibilidadePreco
					, A.VolumeFinal_MWh
					, A.VolumeFinal_MWm
					, A.precoContrato
					, A.PrecoFinal
					, A.Spread
					, A.TetoPreco
					, A.PisoPreco
					, B.Preco
					, C.Curva
					, C.Preco_PLD
					, C.Preco_Energia
					, (
						CASE	
							WHEN A.NaturezaOperacao = 'Compra' THEN -1
							WHEN A.NaturezaOperacao = 'Venda' THEN 1
						END
					) * ROUND(
						(A.VolumeFinal_Mwh) * (
							CASE
								WHEN A.FlexibilidadePreco = 'Fixo' 
								THEN (A.PrecoFinal)-((ISNULL(c.Preco_Energia, b .Preco)))
								
								WHEN A.FlexibilidadePreco = 'Variável' 
								THEN (
									CASE 
										WHEN 
											A.TetoPreco IS NULL 
											AND A.PisoPreco IS NULL 
										THEN 
											ISNULL (c.Preco_PLD, b.Preco) + A.Spread - (ISNULL(c.Preco_Energia, b .Preco)) --Variavel normal
										
										WHEN A.TetoPreco > 0 
											AND A.PisoPreco > 0 
										THEN (
											CASE 
												WHEN A.TetoPreco > 0 
													AND (ISNULL(c.Preco_Energia, b .Preco)) >= A.TetoPreco 
												THEN A.TetoPreco --Collar

												WHEN A.PisoPreco > 0 
													AND (ISNULL(c.Preco_Energia, b.Preco)) <= A.PisoPreco
												THEN A.PisoPreco
												
												ELSE ISNULL(c.Preco_PLD, b.Preco) + A.Spread
											END
										) - (ISNULL(c.Preco_PLD, b .Preco))
										
										WHEN A.TetoPreco > 0 
											OR A.PisoPreco > 0 
										THEN (
											CASE 
												WHEN A.TetoPreco > 0 
												THEN (
													CASE 
														WHEN (ISNULL(c.Preco_PLD, b .Preco)) + A.Spread >= A.TetoPreco 
														THEN A.TetoPreco
														ELSE (ISNULL(c.Preco_PLD, b .Preco)) + A.Spread
													END
												) --Opcao Com Teto
																																				
												WHEN A.PisoPreco > 0 
												THEN (
													CASE
														WHEN (ISNULL(c.Preco_PLD, b .Preco)) + A.Spread <= A.PisoPreco 
														THEN A.PisoPreco
														ELSE (ISNULL(c.Preco_PLD, b .Preco)) + A.Spread
													END 
												)
											END
										) - (ISNULL(c.Preco_Energia, b .Preco)) --Opcao Com Piso													 
																								
									END 
								)
							END
						)
						,2)	AS Resultado

					--, Book.fn_Resultado(
					--	A.PrecoFinal
					--	, C.Preco_Energia
					--	, B.Preco
					--	, A.TetoPreco
					--	, A.PisoPreco
					--	, C.Preco_PLD
					--	, A.Spread
					--	, A.NaturezaOperacao
					--	, A.FlexibilidadePreco
					--	, A.VolumeFinal_MWh
					--)	AS Resultado

				FROM #TempMaeReduzida AS A
				LEFT JOIN [Book].[Curva].[PLD_Oficial]  AS B
					ON A.DataFornecimento=B.[Data]
						AND A.Submercado=B.Submercado
						AND B.DataInsert <= @CurrentDate --Inserir DataDia
				LEFT JOIN #TempCurvaFwd AS C 
					ON A.DataFornecimento=c.data_fwd
						AND A.Submercado= C.Submercado
						AND A.FonteEnergia = C.Fonte_Energia
						AND c.[Data]= @CurrentDate -- Inserir DataFwd
			) TempLeftJoinCurvaResultado

			INSERT INTO #MyTable (
				DataHistorico				
				, DataFornecimento		
				, NaturezaOperacao    		
				, FonteEnergia				
				, Submercado				
				, Thunders					
				, Classificacao		
				, FlexibilidadePreco		
				, TipoFlexibilidadePreco	
				, VolumeFinal_MWh			
				, VolumeFinal_MWm			
				, PrecoContrato				
				, PrecoFinal				
				, Spread				
				, TetoPreco				
				, PisoPreco				
				, Preco					
				, Curva					
				, Preco_PLD					
				, Preco_Energia			
				, Resultado 
			)
			SELECT * FROM #TempLeftJoinCurvaResultado
		END
		DELETE FROM BOOK.[BOOK].[HistoricoResultado_log] WHERE DataHistorico = @CurrentDate + ' 23:59:59.000'
		SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate); /*increment current date*/
	END

	INSERT INTO BOOK.[BOOK].[HistoricoResultado_log]
	SELECT * FROM #MyTable
		-------------------------------------------------------------------------------------------------------------------------------------------
							-- Merge para inserir em uma tabela física
		-------------------------------------------------------------------------------------------------------------------------------------------

	--IF OBJECT_ID('book.[HistoricoResultado_log]', 'U') IS NOT NULL
	
	--MERGE 
	--	Book.book.[HistoricoResultado_log] AS Destino
	--USING 
	--	#MyTable  AS Origem 
	
	--ON CONVERT(date,Origem.DataHistorico)  = Destino.DataHistorico
	--		AND Origem.DataFornecimento = Destino.DataFornecimento
	--		AND (Origem.Classificacao collate SQL_Latin1_General_CP1_CI_AS)  = (Destino.Classificacao collate SQL_Latin1_General_CP1_CI_AS)

	--WHEN MATCHED THEN
	--	UPDATE 
	--	SET 
	--	 destino.VolumeFinal_MWh = Origem.VolumeFinal_MWh
	--	,destino.VolumeFinal_MWm = Origem.VolumeFinal_MWm
	--	,destino.Resultado		 = Origem.Resultado


 --Registro não existe no destino. Vamos inserir.
	--WHEN NOT MATCHED THEN
	--	INSERT 
	--	VALUES(Origem.DataHistorico				
	--		, Origem.DataFornecimento			
	--		, Origem.NaturezaOperacao    		
	--		, Origem.FonteEnergia				
	--		, Origem.Submercado				
	--		, Origem.Thunders					
	--		, Origem.Classificacao		
	--		, Origem.FlexibilidadePreco		
	--		, Origem.TipoFlexibilidadePreco	
	--		, Origem.VolumeFinal_MWh			
	--		, Origem.VolumeFinal_MWm			
	--		, Origem.PrecoContrato				
	--		, Origem.PrecoFinal				
	--		, Origem.Spread					
	--		, Origem.TetoPreco					
	--		, Origem.PisoPreco					
	--		, Origem.Preco						
	--		, Origem.Curva						
	--		, Origem.Preco_PLD					
	--		, Origem.Preco_Energia				
	--		, Origem.Resultado 	
	--		);

END







GO


