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
				, TipoFlexibilidadePreco
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
				, TipoContrato
				, TipoOperacao
				, NaturezaOperacao
				, TipoFlexibilidadePreco
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
				, TipoFlexibilidadePreco
				, Submercado
				, FonteEnergia
				, FlexibilidadePreco order by precoContrato asc) rak_idx
				
				--FROM [modelo].[BaseHistorica].[temp_HistoricoPosicao_log]
				FROM book.HistoricoPosicao_log

				WHERE DataHistorico >=  (select concat(CONVERT(DATE, DATEADD(DAY, -4, (max(data))), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial')
				) h
			WHERE   h.datafornecimento < (select CONVERT(DATE, DATEADD(month, -6, (GETDATE())), 103) )--'2024-09-01'
			AND  h.rak_idx > 1
		) TempTableLogHistory;

END

select * from #TempTableLogHistory


-- /*** FILTRO **/		
-- SELECT h.* FROM (
-- 				SELECT *, RANK() over(partition by  DataFornecimento
-- 				, isTrading
-- 				, isServices
-- 				, isGeneration
-- 				, Thunders
-- 				, EmpresaResponsavel
-- 				, TipoNegocio
-- 				, Classificacao
-- 				, TipoContrato
-- 				, TipoOperacao
-- 				, NaturezaOperacao
-- 				, TipoFlexibilidadePreco
-- 				, Submercado
-- 				, FonteEnergia
-- 				, FlexibilidadePreco order by precoContrato asc) rak_idx

-- 				FROM book.HistoricoPosicao_log h
-- 				WHERE h.DataHistorico >=  (select concat(CONVERT(DATE, DATEADD(DAY, -4, (max(data))), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial')
-- 				) h
-- 				WHERE  h.datafornecimento = '2025-04-01' --'2023-05-01'
-- 				AND h.thunders = 'Comercial' --'Comercial'
-- 				/**/
-- 				and h.EmpresaResponsavel = 'SAFIRA VAREJO COMERCIALIZACAO DE ENERGIA LTDA'
-- 				and h.classificacao = 'Varejista' --'Sfr2'
-- 				and h.tiponegocio = 'Negocio Externo' --'Negocio Interno'
-- 				and h.tipocontrato = 'Físico'
--                 and h.tipooperacao = 'Economia Garantida'
-- 				and h.naturezaoperacao = 'Venda' --'Compra'
-- 				and h.submercado = 'SE'
-- 				and h.fonteenergia = '100% Incent.'
-- 				and h.flexibilidadepreco = 'Fixo' --'Variável'
-- 				and h.TipoFlexibilidadePreco = 'Fixo'
-- 				/**/
-- 				--and  rak_idx > 1
-- 				ORDER BY precoContrato DESC


-- /** VALIDACAO TABELA RESULTADO DA FUNCTION Book.dbo.s_all_operations(@DataHistorico)
-- SELECT h.* FROM (
-- 				SELECT *, RANK() over(partition by  DataFornecimento
-- 				, isTrading
-- 				, isServices
-- 				, isGeneration
-- 				, Thunders
-- 				, EmpresaResponsavel
-- 				, TipoNegocio
-- 				, Classificacao
-- 				, TipoOperacao
-- 				, NaturezaOperacao
-- 				, Submercado
-- 				, FonteEnergia
-- 				, FlexibilidadePreco order by precoContrato asc) rak_idx
-- 				FROM [modelo].[BaseHistorica].[BoletasProcessadas] --book.HistoricoPosicao_log
-- 				WHERE DataHistorico >=  (select concat(CONVERT(DATE, DATEADD(DAY, -4, (max(data))), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial')

-- 				) h
-- 				WHERE h.DataFornecimento = '2023-03-01' --'2023-05-01'
-- 				AND h.Thunders = 'Safira' --'Comercial'
-- 				/**/
-- 				and h.classificacao = 'DIRECIONAL' --'Sfr2'
-- 				and h.tiponegocio = 'Negocio Externo' --'Negocio Interno'
-- 				and h.naturezaoperacao = 'Venda' --'Compra'
-- 				and h.submercado = 'SE'
-- 				and h.fonteenergia = 'Convencional'
-- 				and h.flexibilidadepreco = 'Fixo' --'Variável'
-- 				/**/
-- 				--and  rak_idx > 1
-- 				ORDER BY precoContrato DESC

-- **/


-- BEGIN

-- SET NOCOUNT ON;
-- SET ANSI_WARNINGS OFF;

-- 		DECLARE @DataLogHistorico as varchar(100);
-- 		SET @DataLogHistorico = (SELECT CONVERT(VARCHAR(100), SYSDATETIME(), 121));

-- 		-- LOG
-- 		IF OBJECT_ID('tempdb..#TempTableLogHistory') IS NOT NULL
-- 		BEGIN
-- 			DROP TABLE #TempTableLogHistory;
-- 		END

-- 		-- Tabela log
-- 		SELECT * INTO #TempTableLogHistory
-- 		FROM (
-- 			SELECT * FROM  (
-- 			SELECT 
-- 				ROW_NUMBER() over(partition by  DataFornecimento
-- 				, isTrading
-- 				, isServices
-- 				, isGeneration
-- 				, Thunders
-- 				, EmpresaResponsavel
-- 				, TipoNegocio
-- 				, Classificacao
-- 				--, TipoContrato
-- 				, TipoOperacao
-- 				, NaturezaOperacao
-- 				, TipoFlexibilidadePreco
-- 				, Submercado
-- 				, FonteEnergia
-- 				, FlexibilidadePreco order by DataHistorico asc) rown_idx
-- 				, DataFornecimento
-- 				, isTrading
-- 				, isServices
-- 				, isGeneration
-- 				, Thunders
-- 				, EmpresaResponsavel
-- 				, TipoNegocio
-- 				, Classificacao
-- 				, TipoContrato
-- 				, TipoOperacao
-- 				, NaturezaOperacao
-- 				, TipoFlexibilidadePreco
-- 				, Submercado
-- 				, FonteEnergia
-- 				, FlexibilidadePreco
-- 				, precoContrato
-- 				, DataHistorico
-- 				--, DENSE_RANK() over(partition by   datafornecimento,thunders,classificacao,tiponegocio, tipocontrato, tipooperacao, naturezaoperacao, submercado, fonteenergia, flexibilidadepreco order by precoContrato asc) des_rak_idx

-- 				, RANK() over(partition by  DataFornecimento
-- 				, isTrading
-- 				, isServices
-- 				, isGeneration
-- 				, Thunders
-- 				, EmpresaResponsavel
-- 				, TipoNegocio
-- 				, Classificacao
-- 				--, TipoContrato
-- 				, TipoOperacao
-- 				, NaturezaOperacao
-- 				, TipoFlexibilidadePreco
-- 				, Submercado
-- 				, FonteEnergia
-- 				, FlexibilidadePreco order by precoContrato asc) rak_idx
				
-- 				FROM [modelo].[BaseHistorica].[temp_HistoricoPosicao_log]
-- 				WHERE DataHistorico >=  (select concat(CONVERT(DATE, DATEADD(DAY, -4, (max(data))), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial')
-- 				) h
-- 			WHERE   h.datafornecimento < (select CONVERT(DATE, DATEADD(month, -6, (GETDATE())), 103) )--'2024-09-01'
-- 			AND  h.rak_idx > 1
-- 		) TempTableLogHistory;

-- END

-- select * from #TempTableLogHistory



-- /*** FILTRO **/		
-- SELECT h.* FROM (
-- 				SELECT *, RANK() over(partition by  DataFornecimento
-- 				, isTrading
-- 				, isServices
-- 				, isGeneration
-- 				, Thunders
-- 				, EmpresaResponsavel
-- 				, TipoNegocio
-- 				, Classificacao
-- 				, TipoOperacao
-- 				, NaturezaOperacao
-- 				, TipoFlexibilidadePreco
-- 				, Submercado
-- 				, FonteEnergia
-- 				, FlexibilidadePreco order by precoContrato asc) rak_idx

-- 				FROM [modelo].[BaseHistorica].[temp_HistoricoPosicao_log] h
-- 				WHERE h.DataHistorico >=  (select concat(CONVERT(DATE, DATEADD(DAY, -4, (max(data))), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial')
-- 				) h
-- 				WHERE  h.datafornecimento = '2023-02-01' --'2023-05-01'
-- 				AND h.thunders = 'Safira' --'Comercial'
-- 				/**/
-- 				and h.EmpresaResponsavel = 'SAFIRA ADMINISTRACAO E COMERCIALIZACAO DE ENERGIA S.A.'
-- 				and h.tiponegocio = 'IC' --'Negocio Interno'
-- 				and h.classificacao = 'DIRECIONAL' --'Sfr2'
--                 and h.tipooperacao = 'Venda'
-- 				and h.naturezaoperacao = 'Venda' --'Compra'
-- 				and h.TipoFlexibilidadePreco = 'Fixo'
-- 				and h.submercado = 'SE'
-- 				and h.fonteenergia = 'Convencional'
-- 				and h.flexibilidadepreco = 'Fixo' --'Variável'
				
-- 				/**/
-- 				--and  rak_idx > 1
-- 				ORDER BY precoContrato DESC
