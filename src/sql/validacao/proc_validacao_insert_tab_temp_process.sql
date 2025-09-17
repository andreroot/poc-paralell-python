BEGIN

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

		DECLARE @DataLogHistorico as varchar(100);
		SET @DataLogHistorico = (SELECT CONVERT(VARCHAR(100), SYSDATETIME(), 121));

		-- LOG
		IF OBJECT_ID('tempdb..#TempTableLogHistory') IS NOT NULL
		BEGIN
			DROP TABLE #TempTableLogHistory;
		END

		-- Tabela log
		SELECT * INTO #TempTableLogHistory
		FROM (
			-- SELECT h.Thunders
			-- 	, h.DataHistorico
			-- 	, h.empresaResponsavel
			-- 	, h.rak_idx
			-- 	, min(h.datafornecimento) MinDataFornecimento
			-- 	--, sum(h.VolumeFinal_MWh) TotalVolumeFinal_MWh
			-- 	--, sum(h.VolumeFinal_MWm) TotalVolumeFinal_MWm
			-- 	, sum(h.PrecoContrato) TotalPrecoContrato
			-- 	--, sum(h.PrecoFinal) TotalPrecoFinal
			-- 	, count(1) QtdeRows
			SELECT * FROM  (
			SELECT 
				ROW_NUMBER() over(partition by  DataFornecimento
				, isTrading
				, isServices
				, isGeneration
				, Thunders
				, EmpresaResponsavel
				, TipoNegocio
				, Classificacao
				--, TipoContrato
				, TipoOperacao
				, NaturezaOperacao
				--, TipoFlexibilidadePreco
				, Submercado
				, FonteEnergia
				, FlexibilidadePreco order by DataHistorico asc) rown_idx
				, DataFornecimento
				, isTrading
				, isServices
				, isGeneration
				, Thunders
				, EmpresaResponsavel
				, TipoNegocio
				, Classificacao
				--, TipoContrato
				, TipoOperacao
				, NaturezaOperacao
				--, TipoFlexibilidadePreco
				, Submercado
				, FonteEnergia
				, FlexibilidadePreco
				, precoContrato
				, DataHistorico
				--, DENSE_RANK() over(partition by   datafornecimento,thunders,classificacao,tiponegocio, tipocontrato, tipooperacao, naturezaoperacao, submercado, fonteenergia, flexibilidadepreco order by precoContrato asc) des_rak_idx

				, RANK() over(partition by  DataFornecimento
				, isTrading
				, isServices
				, isGeneration
				, Thunders
				, EmpresaResponsavel
				, TipoNegocio
				, Classificacao
				--, TipoContrato
				, TipoOperacao
				, NaturezaOperacao
				--, TipoFlexibilidadePreco
				, Submercado
				, FonteEnergia
				, FlexibilidadePreco order by precoContrato asc) rak_idx
				
			FROM [modelo].[BaseHistorica].[BoletasProcessadas]
				 WHERE BoletaAtiva = 1
				-- Remove as boletas apagadas
				   AND UnidadeNegocio != 'Serviços'
				-- Seleciona Só as boletas mais recentes
				   AND year(DataFornecimento) >= Year(GETDATE()) -1
				  -- and month(DataFornecimento) = 9
				  AND DataHistorico >=  (select concat(CONVERT(DATE, DATEADD(DAY, -4, (max(data))), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial')
				) h
			WHERE   h.datafornecimento < (select CONVERT(DATE, DATEADD(month, -6, (GETDATE())), 103) )--'2024-09-01'
			AND  h.rak_idx > 1
			-- GROUP BY h.Thunders
			-- 	, h.DataHistorico
			-- 	, h.empresaResponsavel
			-- 	, h.rak_idx
		) TempTableLogHistory;


		--INSERT INTO [modelo].[BaseHistorica].[LogDataDiffHistPosicaoLog]
		-- SELECT Thunders
		-- 	, DataHistorico
		-- 	, empresaResponsavel
		-- 	, MinDataFornecimento
		-- 	--, TotalVolumeFinal_MWh
		-- 	--, TotalVolumeFinal_MWm
		-- 	, TotalPrecoContrato
		-- 	--, TotalPrecoFinal
		-- 	, QtdeRows
		-- FROM #TempTableLogHistory
		-- ORDER BY Thunders
		-- 	, MinDataFornecimento
		-- 	, DataHistorico
		-- 	, empresaResponsavel;



END

select * from #TempTableLogHistory

/*** FILTRO **/		
SELECT h.* FROM (
				SELECT *, RANK() over(partition by  DataFornecimento
				, isTrading
				, isServices
				, isGeneration
				, Thunders
				, EmpresaResponsavel
				, TipoNegocio
				, Classificacao
				--, TipoContrato
				, TipoOperacao
				, NaturezaOperacao
				--, TipoFlexibilidadePreco
				, Submercado
				, FonteEnergia
				, FlexibilidadePreco order by precoContrato asc) rak_idx

			FROM [modelo].[BaseHistorica].[BoletasProcessadas]
				 WHERE BoletaAtiva = 1
				-- Remove as boletas apagadas
				   AND UnidadeNegocio != 'Serviços'
				-- Seleciona Só as boletas mais recentes
				   AND year(DataFornecimento) >= Year(GETDATE()) -1
				  AND  DataHistorico >=  (select concat(CONVERT(DATE, DATEADD(DAY, -4, (max(data))), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial')
				) h
				WHERE  h.datafornecimento = '2023-07-01' --'2023-05-01'
				AND h.thunders = 'Safira' --'Comercial'
				/**/
				and h.EmpresaResponsavel = 'SAFIRA ADMINISTRACAO E COMERCIALIZACAO DE ENERGIA S.A.'
				and h.classificacao = 'ESTRATEGIA MESA' --'Sfr2'
				and h.tiponegocio = 'Negocio Externo' --'Negocio Interno'
                and h.tipooperacao = 'Compra'
				and h.naturezaoperacao = 'Compra' --'Compra'
				and h.submercado = 'SE'
				and h.fonteenergia = 'Convencional'
				and h.flexibilidadepreco = 'Fixo' --'Variável'
				--and h.TipoFlexibilidadePreco = 'Fixo'
				/**/
				--and  rak_idx > 1
				ORDER BY precoContrato DESC


/** VALIDACAO TABELA RESULTADO DA FUNCTION Book.dbo.s_all_operations(@DataHistorico)
SELECT h.* FROM (
				SELECT *, RANK() over(partition by  DataFornecimento
				, isTrading
				, isServices
				, isGeneration
				, Thunders
				, EmpresaResponsavel
				, TipoNegocio
				, Classificacao
				, TipoOperacao
				, NaturezaOperacao
				, Submercado
				, FonteEnergia
				, FlexibilidadePreco order by precoContrato asc) rak_idx
				FROM [modelo].[BaseHistorica].[BoletasProcessadas] --book.HistoricoPosicao_log
				WHERE DataHistorico >=  (select concat(CONVERT(DATE, DATEADD(DAY, -4, (max(data))), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial')

				) h
				WHERE h.DataFornecimento = '2023-03-01' --'2023-05-01'
				AND h.Thunders = 'Safira' --'Comercial'
				/**/
				and h.classificacao = 'DIRECIONAL' --'Sfr2'
				and h.tiponegocio = 'Negocio Externo' --'Negocio Interno'
				and h.naturezaoperacao = 'Venda' --'Compra'
				and h.submercado = 'SE'
				and h.fonteenergia = 'Convencional'
				and h.flexibilidadepreco = 'Fixo' --'Variável'
				/**/
				--and  rak_idx > 1
				ORDER BY precoContrato DESC



select top 30 * from [modelo].[BaseHistorica].[LogBoletasProcessadas] order by ProcessInsertTimeFim desc


SELECT * 
FROM  Modelo.dbo.proc_POC_Historico_portfolio_d0 WHERE DataHistorico = (select DATEADD(DAY, -1, max(data)) start_date from book.curva.Curva_Fwd where curva = 'Oficial')

		SELECT DataFornecimento, Thunders, DataHistorico, count(1) QtdeRows
				, SUM(VolumeFinal_MWh) AS VolumeFinal_MWh
				, SUM(VolumeFinal_MWm) AS VolumeFinal_MWm
				, SUM(VolumeFinal_MWh * PrecoContrato) / NULLIF( SUM(VolumeFinal_MWh),0) AS PrecoContrato
				, SUM(VolumeFinal_MWh * PrecoFinal) / NULLIF( SUM(VolumeFinal_MWh),0) AS PrecoFinal
				, SUM(VolumeFinal_MWh * Spread) / NULLIF( SUM(VolumeFinal_MWh),0) AS Spread
				, SUM(TetoPreco) AS TetoPreco
				, SUM(PisoPreco) AS PisoPreco
			FROM book.HistoricoPosicao_log
				 WHERE  DataHistorico >= (select concat(CONVERT(DATE, DATEADD(DAY, -4, (max(data))), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial')
				   AND year(DataFornecimento) = Year(GETDATE()) -1
				  -- and month(DataFornecimento) = 9
			GROUP BY DataFornecimento, Thunders, DataHistorico
			ORDER BY DataFornecimento, Thunders, DataHistorico DESC


SELECT COUNT(1)			FROM book.HistoricoPosicao_log
--WHERE DataHistorico =  (select concat(CONVERT(DATE, DATEADD(DAY, -1, (max(data))), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial')
WHERE DataHistorico =  (select concat(max(data),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial')

SELECT DataHistorico, COUNT(1)			FROM book.HistoricoPosicao_log
WHERE DataHistorico >=  (select concat(CONVERT(DATE, DATEADD(DAY, -1, (max(data))), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial')
--20467
GROUP BY DataHistorico

		SELECT DataFornecimento, Thunders, DataHistorico, count(1) QtdeRows
				, SUM(VolumeFinal_MWh) AS VolumeFinal_MWh
				, SUM(VolumeFinal_MWm) AS VolumeFinal_MWm
				, SUM(VolumeFinal_MWh * PrecoContrato) / NULLIF( SUM(VolumeFinal_MWh),0) AS PrecoContrato
				, SUM(VolumeFinal_MWh * PrecoFinal) / NULLIF( SUM(VolumeFinal_MWh),0) AS PrecoFinal
				, SUM(VolumeFinal_MWh * Spread) / NULLIF( SUM(VolumeFinal_MWh),0) AS Spread
				, SUM(TetoPreco) AS TetoPreco
				, SUM(PisoPreco) AS PisoPreco
			FROM [modelo].[BaseHistorica].[BoletasProcessadas]
				 WHERE BoletaAtiva = 1
				-- Remove as boletas apagadas
				   AND UnidadeNegocio != 'Serviços'
				-- Seleciona Só as boletas mais recentes
				   AND year(DataFornecimento) = Year(GETDATE()) -1
				  -- and month(DataFornecimento) = 9
			AND DataHistorico >= (select concat(CONVERT(DATE, DATEADD(DAY, -4, (max(data))), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial')
			GROUP BY DataFornecimento, Thunders, DataHistorico
			ORDER BY DataFornecimento, Thunders, DataHistorico DESC
**/




BEGIN

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

		DECLARE @DataLogHistorico as varchar(100);
		SET @DataLogHistorico = (SELECT CONVERT(VARCHAR(100), SYSDATETIME(), 121));

		-- LOG
		IF OBJECT_ID('tempdb..#TempTableLogHistory') IS NOT NULL
		BEGIN
			DROP TABLE #TempTableLogHistory;
		END

		-- Tabela log
		SELECT * INTO #TempTableLogHistory
		FROM (

			SELECT * FROM  (
			SELECT 
				ROW_NUMBER() over(partition by  DataFornecimento
				, isTrading
				, isServices
				, isGeneration
				, Thunders
				, EmpresaResponsavel
				, TipoNegocio
				, Classificacao
				--, TipoContrato
				, TipoOperacao
				, NaturezaOperacao
				--, TipoFlexibilidadePreco
				, Submercado
				, FonteEnergia
				, FlexibilidadePreco order by DataHistorico asc) rown_idx
				, DataFornecimento
				, isTrading
				, isServices
				, isGeneration
				, Thunders
				, EmpresaResponsavel
				, TipoNegocio
				, Classificacao
				--, TipoContrato
				, TipoOperacao
				, NaturezaOperacao
				--, TipoFlexibilidadePreco
				, Submercado
				, FonteEnergia
				, FlexibilidadePreco
				, precoContrato
				, DataHistorico
				--, DENSE_RANK() over(partition by   datafornecimento,thunders,classificacao,tiponegocio, tipocontrato, tipooperacao, naturezaoperacao, submercado, fonteenergia, flexibilidadepreco order by precoContrato asc) des_rak_idx

				, RANK() over(partition by  DataFornecimento
				, isTrading
				, isServices
				, isGeneration
				, Thunders
				, EmpresaResponsavel
				, TipoNegocio
				, Classificacao
				--, TipoContrato
				, TipoOperacao
				, NaturezaOperacao
				--, TipoFlexibilidadePreco
				, Submercado
				, FonteEnergia
				, FlexibilidadePreco order by precoContrato asc) rak_idx
				
			FROM [modelo].[BaseHistorica].[BoletasProcessadas]
				 WHERE BoletaAtiva = 1
				-- Remove as boletas apagadas
				   AND UnidadeNegocio != 'Serviços'
				-- Seleciona Só as boletas mais recentes
				   AND year(DataFornecimento) >= Year(GETDATE()) -1
				  -- and month(DataFornecimento) = 9
				  AND DataHistorico >=  (select concat(CONVERT(DATE, DATEADD(DAY, -4, (max(data))), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial')
				) h
			WHERE   h.datafornecimento < (select CONVERT(DATE, DATEADD(month, -6, (GETDATE())), 103) )--'2024-09-01'
			AND  h.rak_idx > 1
			-- GROUP BY h.Thunders
			-- 	, h.DataHistorico
			-- 	, h.empresaResponsavel
			-- 	, h.rak_idx
		) TempTableLogHistory;




END

select DataHistorico, DataFornecimento
				, isTrading
				, isServices
				, isGeneration
				, Thunders
				, EmpresaResponsavel
				, TipoNegocio
				, Classificacao
				--, TipoContrato
				, TipoOperacao
				, NaturezaOperacao
				--, TipoFlexibilidadePreco
				, Submercado
				, FonteEnergia
				, FlexibilidadePreco
				, SUM(precoContrato)
from #TempTableLogHistory
GROUP BY 
DataHistorico, DataFornecimento
				, isTrading
				, isServices
				, isGeneration
				, Thunders
				, EmpresaResponsavel
				, TipoNegocio
				, Classificacao
				--, TipoContrato
				, TipoOperacao
				, NaturezaOperacao
				--, TipoFlexibilidadePreco
				, Submercado
				, FonteEnergia
				, FlexibilidadePreco

/*** FILTRO **/		
SELECT h.* FROM (
				SELECT *, RANK() over(partition by  DataFornecimento
				, isTrading
				, isServices
				, isGeneration
				, Thunders
				, EmpresaResponsavel
				, TipoNegocio
				, Classificacao
				--, TipoContrato
				, TipoOperacao
				, NaturezaOperacao
				--, TipoFlexibilidadePreco
				, Submercado
				, FonteEnergia
				, FlexibilidadePreco order by precoContrato asc) rak_idx

			FROM [modelo].[BaseHistorica].[BoletasProcessadas]
				 WHERE BoletaAtiva = 1
				-- Remove as boletas apagadas
				   AND UnidadeNegocio != 'Serviços'
				-- Seleciona Só as boletas mais recentes
				   AND year(DataFornecimento) >= Year(GETDATE()) -1
				  AND  DataHistorico >=  (select concat(CONVERT(DATE, DATEADD(DAY, -4, (max(data))), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial')
				) h
				WHERE  h.datafornecimento = '2023-07-01' --'2023-05-01'
				AND h.thunders = 'Safira' --'Comercial'
				/**/
				and h.EmpresaResponsavel = 'SAFIRA ADMINISTRACAO E COMERCIALIZACAO DE ENERGIA S.A.'
				and h.classificacao = 'ESTRATEGIA MESA' --'Sfr2'
				and h.tiponegocio = 'Negocio Externo' --'Negocio Interno'
                and h.tipooperacao = 'Compra'
				and h.naturezaoperacao = 'Compra' --'Compra'
				and h.submercado = 'SE'
				and h.fonteenergia = 'Convencional'
				and h.flexibilidadepreco = 'Fixo' --'Variável'
				--and h.TipoFlexibilidadePreco = 'Fixo'
				/**/
				--and  rak_idx > 1
				ORDER BY precoContrato DESC

