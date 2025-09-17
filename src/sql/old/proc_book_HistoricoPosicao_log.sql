BEGIN

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

DECLARE @DataHistorico as varchar(100);
SET @DataHistorico = (select concat(max(data),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial');

-- Um ponto a se destacar é que para Datas MENORES do que 24-11-2023 é preciso utilizar UNIDADE DE NEGOCIO IN ('Trading')
-- Isso acontece porque as operações estão duplicadas porque colocamos o módulo para funcionar.
		-----------------------------------------------------------------------------------------------
			--Cria uma tabela geral UNION para o histórico
			IF OBJECT_ID('tempdb..#TempTable') IS NOT NULL
			BEGIN
			   DROP TABLE #TempTable;
			END
			-- Note que foi colocado um 'Físico as TipoContrato' porque a procedure não está funcionando isso...
			SELECT * INTO #TempTable
			FROM (
                -- alterado para tabela via proc all history
				SELECT *,'Físico' as TipoContrato 
				  FROM [modelo].[BaseHistorica].[BoletasProcessadasv2]
				 WHERE BoletaAtiva = 1
				-- Remove as boletas apagadas
				   AND UnidadeNegocio != 'Serviços'
				-- Seleciona Só as boletas mais recentes
				   AND year(DataFornecimento) >= Year(GETDATE()) - 1
				   AND Thunders in ('Safira', 'Comercial', 'Indra' )
				   AND DataHistorico = CONVERT(DATE, @dataHistorico, 103) --@dataHistorico
			) TempTable;

		---------------------------------------------------------------------------------------------------------------------
		--Reduzir a tabela mãe
			IF OBJECT_ID('tempdb..#TempTable1') IS NOT NULL
			BEGIN
			   DROP TABLE #TempTable1
			END
			-- Tabela para reduzir e criar tabela "Mãe"

			SELECT * INTO #TempTable1
			FROM (
				SELECT 
							  DataFornecimento
							, isTrading
							, isServices
							, isGeneration
							, Thunders
							, EmpresaResponsavel
							, TipoNegocio
							, Classificacao
							, TipoContrato
							, TipoOperacao
							, NaturezaOperacao --É preciso fazer um tratamento se entrar recompra.
							, Submercado
							, FonteEnergia
							, FlexibilidadePreco
							, (CASE	WHEN FlexibilidadePreco = 'Fixo' THEN FlexibilidadePreco
									WHEN FlexibilidadePreco = 'Variável' THEN (CASE WHEN TetoPreco is null AND PisoPreco is null THEN FlexibilidadePreco --Variavel normal
																					 WHEN TetoPreco > 0 AND PisoPreco > 0 THEN 'Collar'
																					 WHEN TetoPreco > 0 OR PisoPreco > 0 THEN 'Opção'												 
																				  END )
										END) AS TipoFlexibilidadePreco
							, PrecoContrato
							, PrecoFinal
							, TetoPreco
							, PisoPreco
							, Spread
							, VolumeFinal_MWh
							, VolumeFinal_MWm 
						FROM #TempTable
				) TempTable1

		--SELECT * FROM #TempTable1
		--------------------------------------------------------------------------------------------------------------------------------------
		-- Select para selecionar preço médio das boletas de preço fixo e preço variável
			-- Note que temos boletas em OPCAO, é preciso seleciona-las também, 

		IF OBJECT_ID('tempdb..#TempTable2') IS NOT NULL
		BEGIN
		   DROP TABLE #TempTable2
		END
		-- Tabela com os preços Médios e com as separações de Fixo/Variavel/Collar/Opção
		SELECT 
			@DataHistorico AS DataHistorico
			,* INTO #TempTable2
		FROM (
			-- Esse select só pega FIxo/Variavel
			SELECT
				  DataFornecimento
				, isTrading
				, isServices
				, isGeneration
				, Thunders
				, EmpresaResponsavel
				, TipoNegocio
				, Classificacao
				, TipoContrato
				, TipoOperacao
				, NaturezaOperacao
				, Submercado
				, FonteEnergia
				, FlexibilidadePreco
				, TipoFlexibilidadePreco
				/** VALIDACAO
				, SUM(VolumeFinal_MWh)-0.001 AS VolumeFinal_MWh
				, SUM(VolumeFinal_MWm)-0.001 AS VolumeFinal_MWm
				, SUM(VolumeFinal_MWh * PrecoContrato) / NULLIF( SUM(VolumeFinal_MWh),0)-0.001 AS PrecoContrato
				, SUM(VolumeFinal_MWh * PrecoFinal) / NULLIF( SUM(VolumeFinal_MWh),0)-0.001 AS PrecoFinal
				, SUM(VolumeFinal_MWh * Spread) / NULLIF( SUM(VolumeFinal_MWh),0)-0.001 AS Spread
				, SUM(TetoPreco)-0.001 AS TetoPreco
				, SUM(PisoPreco)-0.001 AS PisoPreco
				**/				
				, SUM(VolumeFinal_MWh) AS VolumeFinal_MWh
				, SUM(VolumeFinal_MWm) AS VolumeFinal_MWm
				, SUM(VolumeFinal_MWh * PrecoContrato) / NULLIF( SUM(VolumeFinal_MWh),0) AS PrecoContrato
				, SUM(VolumeFinal_MWh * PrecoFinal) / NULLIF( SUM(VolumeFinal_MWh),0) AS PrecoFinal
				, SUM(VolumeFinal_MWh * Spread) / NULLIF( SUM(VolumeFinal_MWh),0) AS Spread
				, SUM(TetoPreco) AS TetoPreco
				, SUM(PisoPreco) AS PisoPreco
			FROM #TempTable1
			WHERE TipoFlexibilidadePreco  IN ('Fixo','Variável')
			GROUP BY
				  DataFornecimento
				, isTrading
				, isServices
				, isGeneration
				, Thunders
				, EmpresaResponsavel
				, TipoNegocio
				, Classificacao
				, TipoContrato
				, TipoOperacao
				, NaturezaOperacao
				, TipoFlexibilidadePreco
				, Submercado
				, FonteEnergia
				, FlexibilidadePreco

			UNION ALL
	 
			-- Esse select considera apenas o COLLAR e a OPCAO, serve para ajudar no VaR, já que será por contrato.
			SELECT
				 DataFornecimento
				, isTrading
				, isServices
				, isGeneration
				, Thunders
				, EmpresaResponsavel
				, TipoNegocio
				, Classificacao
				, TipoContrato
				, TipoOperacao
				, NaturezaOperacao
				, Submercado
				, FonteEnergia
				, FlexibilidadePreco
				, TipoFlexibilidadePreco
				, VolumeFinal_MWh AS VolumeFinal_MWh
				, VolumeFinal_MWm AS VolumeFinal_MWm
				, PrecoContrato AS PrecoContrato
				, PrecoFinal  AS PrecoFinal
				, Spread AS Spread
				, TetoPreco
				, PisoPreco
			FROM #TempTable1
			WHERE TipoFlexibilidadePreco IN ('Collar','Opção')

			) TempTable2;

		DELETE FROM book.HistoricoPosicao_log  where DataHistorico = @dataHistorico;
		
		INSERT INTO book.HistoricoPosicao_log 		
			SELECT * FROM #TempTable2;


		COMMIT;

IF OBJECT_ID('tempdb..#TempBoletas') IS NOT NULL
BEGIN
    DROP TABLE #TempBoletas;
END

SELECT * INTO #TempBoletas
  FROM (
		SELECT *
		 FROM book.HistoricoPosicao_log
		WHERE DataHistorico = @DataHistorico
		) TempBoletas;
--WHERE a.DataCriacao = '';

SELECT * FROM #TempBoletas;

END

