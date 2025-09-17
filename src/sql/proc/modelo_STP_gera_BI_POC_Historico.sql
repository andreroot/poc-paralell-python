USE [Modelo]
GO
/****** Object:  StoredProcedure [dbo].[STP_gera_BI_POC_Historico]    Script Date: 06/03/2025 12:09:05 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[STP_gera_BI_POC_Historico] AS
BEGIN
	DECLARE @data_historico_d0 AS DATETIME2 = (SELECT TOP 1 DataHistorico FROM Modelo.dbo.proc_POC_Historico_portfolio_d0);
	DECLARE @data_historico_d1 AS DATETIME2 = (SELECT TOP 1 DataHistorico FROM Modelo.dbo.proc_POC_Historico_portfolio_d1);


	DROP TABLE IF EXISTS Modelo.dbo.proc_POC_Historico_diff_BI;
	WITH base AS (
		SELECT
			Thunders = COALESCE(d0.Thunders, d1.Thunders)
			, UnidadeNegocio = COALESCE(d0.UnidadeNegocio, d1.UnidadeNegocio)
			, Codigo = COALESCE(d0.Codigo, d1.Codigo)
			, Entrega = COALESCE(d0.Entrega, d1.Entrega)
			, DataFornecimento = COALESCE(d0.DataFornecimento, d1.DataFornecimento)
			, MudancaDataFornecimento = CASE
				WHEN d1.Thunders IS NULL THEN 'Data Fornecimento Criada'
				WHEN d0.Thunders IS NULL THEN 'Data Fornecimento Deletada'
			END
			, DIFF_Portfolio =
				CASE
					WHEN COALESCE(d1.Portfolio,'NULL') != COALESCE(d0.Portfolio,'NULL')
					THEN COALESCE(d1.Portfolio,'NULL') + ' -> ' + COALESCE(d0.Portfolio,'NULL')
			END
			, DIFF_Classificacao =
				CASE
					WHEN COALESCE(d1.Classificacao,'NULL') != COALESCE(d0.Classificacao,'NULL')
					THEN COALESCE(d1.Classificacao,'NULL') + ' -> ' + COALESCE(d0.Classificacao,'NULL')
			END
			, DIFF_portfolios =
				CASE
					WHEN COALESCE(d1.portfolios,'NULL') != COALESCE(d0.portfolios,'NULL')
					THEN COALESCE(d1.portfolios,'NULL') + ' -> ' + COALESCE(d0.portfolios,'NULL')
			END

			, BoletaAtiva_d0 = d0.BoletaAtiva
			, BoletaAtiva_d1 = d1.BoletaAtiva
			, DIFF_BoletaAtiva =
				CASE
					WHEN COALESCE(CAST(d1.BoletaAtiva AS VARCHAR),'NULL') != COALESCE(CAST(d0.BoletaAtiva AS VARCHAR),'NULL')
					THEN COALESCE(CAST(d1.BoletaAtiva AS VARCHAR),'NULL') + ' -> ' + COALESCE(CAST(d0.BoletaAtiva AS VARCHAR),'NULL')
			END
			, DIFF_TipoOperacao = 
				CASE
					WHEN COALESCE(d1.TipoOperacao,'NULL') != COALESCE(d0.TipoOperacao,'NULL')
					THEN COALESCE(d1.TipoOperacao,'NULL') + ' -> ' + COALESCE(d0.TipoOperacao,'NULL')
			END
			, DIFF_NaturezaOperacao = 
				CASE
					WHEN COALESCE(d1.NaturezaOperacao,'NULL') != COALESCE(d0.NaturezaOperacao,'NULL')
					THEN COALESCE(d1.NaturezaOperacao,'NULL') + ' -> ' + COALESCE(d0.NaturezaOperacao,'NULL')
			END
			, DIFF_Submercado = 
				CASE
					WHEN COALESCE(d1.Submercado,'NULL') != COALESCE(d0.Submercado,'NULL')
					THEN COALESCE(d1.Submercado,'NULL') + ' -> ' + COALESCE(d0.Submercado,'NULL')
			END
			, DIFF_FonteEnergia = 
				CASE
					WHEN COALESCE(d1.FonteEnergia,'NULL') != COALESCE(d0.FonteEnergia,'NULL')
					THEN COALESCE(d1.FonteEnergia,'NULL') + ' -> ' + COALESCE(d0.FonteEnergia,'NULL')
			END
			, DIFF_FlexibilidadePreco = 
				CASE
					WHEN COALESCE(d1.FlexibilidadePreco,'NULL') != COALESCE(d0.FlexibilidadePreco,'NULL')
					THEN COALESCE(d1.FlexibilidadePreco,'NULL') + ' -> ' + COALESCE(d0.FlexibilidadePreco,'NULL')
			END
			, DIFF_TipoFlexibilidadePreco = 
				CASE
					WHEN COALESCE(d1.TipoFlexibilidadePreco,'NULL') != COALESCE(d0.TipoFlexibilidadePreco,'NULL')
					THEN COALESCE(d1.TipoFlexibilidadePreco,'NULL') + ' -> ' + COALESCE(d0.TipoFlexibilidadePreco,'NULL')
			END

			, DIFF_VolumeFinal_MWh = ISNULL(d0.VolumeFinal_MWh,0) - ISNULL(d1.VolumeFinal_MWh,0)
			, DIFF_VolumeNet_MWh = (CASE d0.NaturezaOperacao WHEN 'Compra' THEN 1 ELSE -1 END) * ISNULL(d0.VolumeFinal_MWh,0) - (CASE d1.NaturezaOperacao WHEN 'Compra' THEN 1 ELSE -1 END) * ISNULL(d1.VolumeFinal_MWh,0)

			, DIFF_PrecoContrato = ISNULL(d0.PrecoContrato,0) - ISNULL(d1.PrecoContrato,0)
			, DIFF_PrecoFinal = ISNULL(d0.PrecoFinal,0) - ISNULL(d1.PrecoFinal,0)
			, DIFF_TetoPreco = ISNULL(d0.TetoPreco,0) - ISNULL(d1.TetoPreco,0)
			, DIFF_PisoPreco = ISNULL(d0.PisoPreco,0) - ISNULL(d1.PisoPreco,0)
			, DIFF_Spread = ISNULL(d0.Spread,0) - ISNULL(d1.Spread,0)

			, VolumeFinal_MWh_d1 = d1.VolumeFinal_MWh
			, VolumeFinal_MWh_d0 = d0.VolumeFinal_MWh
		
			, PrecoContrato_d0 = d0.PrecoContrato
			, PrecoContrato_d1 = d1.PrecoContrato

			, PrecoFinal_d0 = d0.PrecoFinal
			, PrecoFinal_d1 = d1.PrecoFinal

			, TetoPreco_d0 = d0.TetoPreco
			, TetoPreco_d1 = d1.TetoPreco

			, PisoPreco_d0 = d0.PisoPreco
			, PisoPreco_d1 = d1.PisoPreco

			, Spread_d0 = d0.Spread
			, Spread_d1 = d1.Spread

			, Portfolio_d0 = d0.Portfolio
			, Portfolio_d1 = d1.Portfolio

			, Submercado_d0 = d0.Submercado
			, Submercado_d1 = d1.Submercado

			, FonteEnergia_d0 = d0.FonteEnergia
			, FonteEnergia_d1 = d1.FonteEnergia

			, TipoFlexibilidadePreco_d0 = d0.TipoFlexibilidadePreco
			, TipoFlexibilidadePreco_d1 = d1.TipoFlexibilidadePreco


			, DIFF_PrecoPLDRef = ISNULL(d0.PrecoPLDRef,0) - ISNULL(d1.PrecoPLDRef,0)
			, DIFF_PrecoFuturoRef = ISNULL(d0.PrecoFuturoRef,0) - ISNULL(d1.PrecoFuturoRef,0)
			, DIFF_V_Curva_referencia = ISNULL(d0.V_Curva_referencia,0) - ISNULL(d1.V_Curva_referencia,0)
			, DIFF_V_Curva_Agio = ISNULL(d0.V_Curva_Agio,0) - ISNULL(d1.V_Curva_Agio,0)
			, DIFF_V_Curva_Spread_Submercado = ISNULL(d0.V_Curva_Spread_Submercado,0) - ISNULL(d1.V_Curva_Spread_Submercado,0)
			, DIFF_V_Curva_spread_fonte_energia = ISNULL(d0.V_Curva_spread_fonte_energia,0) - ISNULL(d1.V_Curva_spread_fonte_energia,0)
			, DIFF_T_PLD_Preco = ISNULL(d0.T_PLD_Preco,0) - ISNULL(d1.T_PLD_Preco,0)
			, DIFF_PrecoComparacao = ISNULL(d0.PrecoComparacao,0) - ISNULL(d1.PrecoComparacao,0)
			, DIFF_PrecoComparacao_Ref = ISNULL(d0.PrecoComparacao_Ref,0) - ISNULL(d1.PrecoComparacao_Ref,0)
			, DIFF_PrecoComparacao_Agio = ISNULL(d0.PrecoComparacao_Agio,0) - ISNULL(d1.PrecoComparacao_Agio,0)
			, DIFF_PrecoComparacao_Spread_Submercado = ISNULL(d0.PrecoComparacao_Spread_Submercado,0) - ISNULL(d1.PrecoComparacao_Spread_Submercado,0)
			, DIFF_PrecoComparacao_Spread_fonte_energia = ISNULL(d0.PrecoComparacao_Spread_fonte_energia,0) - ISNULL(d1.PrecoComparacao_Spread_fonte_energia,0)
			, DIFF_PrecoContratoRef = ISNULL(d0.PrecoContratoRef,0) - ISNULL(d1.PrecoContratoRef,0)


			, PrecoContratoRef_d0 = d0.PrecoContratoRef
			, PrecoContratoRef_d1 = d1.PrecoContratoRef

			, PrecoComparacao_d0 = d0.PrecoComparacao
			, PrecoComparacao_d1 = d1.PrecoComparacao

		
			, PrecoComparacao_Ref_d0 = d0.PrecoComparacao_Ref
			, PrecoComparacao_Ref_d1 = d1.PrecoComparacao_Ref
			, PrecoComparacao_Agio_d0 = d0.PrecoComparacao_Agio
			, PrecoComparacao_Agio_d1 = d1.PrecoComparacao_Agio
			, PrecoComparacao_Spread_Submercado_d0 = d0.PrecoComparacao_Spread_Submercado
			, PrecoComparacao_Spread_Submercado_d1 = d1.PrecoComparacao_Spread_Submercado
			, PrecoComparacao_Spread_fonte_energia_d0 = d0.PrecoComparacao_Spread_fonte_energia
			, PrecoComparacao_Spread_fonte_energia_d1 = d1.PrecoComparacao_Spread_fonte_energia
		

			, Resultado_d0 = d0.Resultado
			, Resultado_d1 = d1.Resultado
			, Resultado2_d0 = d0.Resultado2
			, Resultado2_d1 = d1.Resultado2
			, Resultado3_d0 = d0.Resultado3
			, Resultado3_d1 = d1.Resultado3

		FROM Modelo.dbo.proc_POC_Historico_resultado_d0 d0
		FULL OUTER JOIN Modelo.dbo.proc_POC_Historico_resultado_d1 d1
		ON 
			d0.Thunders = d1.Thunders
			AND d0.UnidadeNegocio = d1.UnidadeNegocio
			AND d0.Codigo = d1.Codigo
			AND d0.Entrega = d1.Entrega
			AND d0.DataFornecimento = d1.DataFornecimento
	),
	entregas AS (
		SELECT
			Thunders = COALESCE(d0.Thunders, d1.Thunders)
			, UnidadeNegocio = COALESCE(d0.UnidadeNegocio, d1.UnidadeNegocio)
			, Codigo = COALESCE(d0.Codigo, d1.Codigo)
			, Entrega = COALESCE(d0.Entrega, d1.Entrega)
			, MudancaEntrega = CASE
				WHEN d1.Thunders IS NULL THEN 'Entrega Criada'
				WHEN d0.Thunders IS NULL THEN 'Entrega Deletada'
			END
		FROM (
			SELECT DISTINCT
					Thunders,
					UnidadeNegocio,
					Codigo,
					Entrega
				FROM Modelo.dbo.proc_POC_Historico_portfolio_d0
			) d0
		FULL OUTER JOIN (
			SELECT DISTINCT
				Thunders,
				UnidadeNegocio,
				Codigo,
				Entrega
			FROM Modelo.dbo.proc_POC_Historico_portfolio_d1
		) d1
		ON 
			d0.Thunders = d1.Thunders
			AND d0.UnidadeNegocio = d1.UnidadeNegocio
			AND d0.Codigo = d1.Codigo
			AND d0.Entrega = d1.Entrega
	),
	boletas AS (
		SELECT
			Thunders = COALESCE(d0.Thunders, d1.Thunders)
			, UnidadeNegocio = COALESCE(d0.UnidadeNegocio, d1.UnidadeNegocio)
			, Codigo = COALESCE(d0.Codigo, d1.Codigo)
			, MudancaBoleta = CASE
				WHEN d1.Thunders IS NULL THEN 'Boleta Criada'
				WHEN d0.Thunders IS NULL THEN 'Boleta Deletada'
			END
		FROM (
			SELECT DISTINCT
				Thunders,
				UnidadeNegocio,
				Codigo
			FROM Modelo.dbo.proc_POC_Historico_portfolio_d0
		) d0
		FULL OUTER JOIN (
			SELECT DISTINCT
				Thunders,
				UnidadeNegocio,
				Codigo
			FROM Modelo.dbo.proc_POC_Historico_portfolio_d1
		) d1
		ON 
			d0.Thunders = d1.Thunders
			AND d0.UnidadeNegocio = d1.UnidadeNegocio
			AND d0.Codigo = d1.Codigo
	),
	diff_registros AS (
		SELECT
			b.*
			, e.MudancaEntrega
			, c.MudancaBoleta
			, Mudanca = COALESCE(
				c.MudancaBoleta,
				e.MudancaEntrega,
				b.MudancaDataFornecimento
			)
			, PrincipalMudanca = CASE
				WHEN c.MudancaBoleta IS NOT NULL THEN c.MudancaBoleta -- Criação ou deleção de registros
				WHEN e.MudancaEntrega IS NOT NULL THEN e.MudancaEntrega -- Criação ou deleção de registros
				WHEN b.MudancaDataFornecimento IS NOT NULL THEN b.MudancaDataFornecimento -- Criação ou deleção de registros
			
				WHEN b.DIFF_Portfolio IS NOT NULL THEN 'Portfólio'
				WHEN b.DIFF_BoletaAtiva = '0 -> 1' THEN 'Boleta Ativada'
				WHEN b.DIFF_BoletaAtiva = '1 -> 0' THEN 'Boleta Inativada'
				WHEN b.DIFF_VolumeFinal_MWh <> 0 AND VolumeFinal_MWh_d0 = 0 THEN 'Boleta Zerada'
				WHEN b.DIFF_VolumeFinal_MWh <> 0 AND VolumeFinal_MWh_d1 = 0 THEN 'Boleta Deszerada'
				WHEN b.DIFF_NaturezaOperacao IS NOT NULL THEN 'Natureza de Operação'
				WHEN b.DIFF_Submercado IS NOT NULL THEN 'Submercado'
				WHEN b.DIFF_FonteEnergia IS NOT NULL THEN 'Fonte de Energia'
				WHEN b.DIFF_FlexibilidadePreco IS NOT NULL THEN 'Flexibilidade de Preço'
				WHEN b.DIFF_TipoFlexibilidadePreco IS NOT NULL THEN 'Tipo Flexibilidade de Preço'
				WHEN b.DIFF_VolumeFinal_MWh <> 0 THEN 'Volume'
				WHEN b.DIFF_PrecoContrato <> 0 THEN 'Preço Contrato'
				WHEN b.DIFF_PrecoFinal <> 0 THEN 'Preço Final'
				WHEN (
					b.DIFF_PisoPreco <> 0
					or b.DIFF_PrecoContrato <> 0
					or b.DIFF_PrecoFinal <> 0
					or b.DIFF_Spread <> 0
					or b.DIFF_TetoPreco <> 0
					or b.DIFF_VolumeFinal_MWh <> 0
					--or DIFF_VolumeFinal_MWm <> 0
				) THEN 'Outros - Campo Numérico'
				WHEN (
					b.DIFF_Portfolio IS NOT NULL
					OR b.DIFF_Classificacao IS NOT NULL
					OR b.DIFF_portfolios IS NOT NULL
					OR b.DIFF_BoletaAtiva IS NOT NULL
					OR b.DIFF_TipoOperacao  IS NOT NULL 
					OR b.DIFF_NaturezaOperacao  IS NOT NULL
					OR b.DIFF_Submercado IS NOT NULL
					OR b.DIFF_FonteEnergia IS NOT NULL
					OR b.DIFF_FlexibilidadePreco IS NOT NULL
					OR b.DIFF_TipoFlexibilidadePreco IS NOT NULL
				) THEN 'Outros - Campo Categórico'
				WHEN (
					b.DIFF_PrecoPLDRef IS NOT NULL
					OR b.DIFF_PrecoFuturoRef IS NOT NULL
					OR b.DIFF_V_Curva_referencia IS NOT NULL
					OR b.DIFF_V_Curva_Agio IS NOT NULL
					OR b.DIFF_V_Curva_Spread_Submercado IS NOT NULL
					OR b.DIFF_V_Curva_spread_fonte_energia IS NOT NULL
					OR b.DIFF_T_PLD_Preco IS NOT NULL
				) THEN 'Curva/PLD'
			END

			, PrincipalMudancaPortfolio = CASE
				WHEN b.DIFF_VolumeFinal_MWh <> 0 THEN 'Volume'
				WHEN b.DIFF_PrecoContrato <> 0 THEN 'Preço Contrato'
				WHEN b.DIFF_PrecoFinal <> 0 THEN 'Preço Final'
				WHEN b.DIFF_TetoPreco <> 0 THEN 'Teto Preço'
				WHEN b.DIFF_PisoPreco <> 0 THEN 'Piso Preço'
				WHEN b.DIFF_Spread <> 0 THEN 'Spread'		
			END

			, PrincipalMudancaCurvaPLD = CASE
				WHEN b.DIFF_T_PLD_Preco <> 0 THEN 'Inclusão de PLD'
				WHEN b.DIFF_V_Curva_referencia <> 0 THEN 'Referência'
				WHEN b.DIFF_V_Curva_Spread_Submercado <> 0 THEN 'Submercado'
				WHEN b.DIFF_V_Curva_spread_fonte_energia <> 0 THEN 'Fonte de Energia'
				WHEN b.DIFF_V_Curva_Agio <> 0 THEN 'Agio'	
			END


			, MultiplicadorBoletaInativada = (
				CASE
					WHEN b.DIFF_BoletaAtiva = '0 -> 1' THEN 0
					WHEN b.DIFF_BoletaAtiva = '1 -> 0' THEN 0
					ELSE 1
				END
			)
			, MultiplicadorRegistroCriadoDeletado = (
				CASE
					WHEN  COALESCE(
						c.MudancaBoleta,
						e.MudancaEntrega,
						b.MudancaDataFornecimento
					) IS NOT NULL
					THEN 0
					ELSE 1
				END
			)
			, MultiplicadorMudancaSubmercado = (
				CASE
					WHEN b.DIFF_Submercado IS NOT NULL THEN 0
					ELSE 1
				END
			)
			, MultiplicadorMudancaFonteEnergia = (
				CASE
					WHEN b.DIFF_FonteEnergia IS NOT NULL THEN 0
					ELSE 1
				END
			)
			, MultiplicadorTipoFlexibilidadePreco = (
				CASE
					WHEN b.DIFF_TipoFlexibilidadePreco IS NOT NULL THEN 0
					ELSE 1
				END
			)
			-- Variaveis Nulas
			-- Se um dos componentes de calculo do resultado for nulo, e não for um registro criado/deletado
			-- O resultado deve ser inválido
			, MultiplicadorVariaveisNulas = (
				CASE
					WHEN
						(b.PrecoContratoRef_d0 IS NULL OR b.PrecoContratoRef_d1 IS NULL)
						AND COALESCE(
							c.MudancaBoleta,
							e.MudancaEntrega,
							b.MudancaDataFornecimento
						) IS NULL
					THEN 0
					WHEN
						(b.PrecoComparacao_d0 IS NULL OR b.PrecoComparacao_d1 IS NULL)
						AND COALESCE(
							c.MudancaBoleta,
							e.MudancaEntrega,
							b.MudancaDataFornecimento
						) IS NULL
					THEN 0
					WHEN
						(b.VolumeFinal_MWh_d0 IS NULL OR b.VolumeFinal_MWh_d1 IS NULL)
						AND COALESCE(
							c.MudancaBoleta,
							e.MudancaEntrega,
							b.MudancaDataFornecimento
						) IS NULL
					THEN 0
					ELSE 1
				END
			)
		FROM base b
		LEFT JOIN entregas e
		ON b.Thunders = e.Thunders
			AND b.UnidadeNegocio = e.UnidadeNegocio
			AND b.Codigo = e.Codigo
			AND b.Entrega = e.Entrega
		LEFT JOIN boletas c
		ON b.Thunders = c.Thunders
			AND b.UnidadeNegocio = c.UnidadeNegocio
			AND b.Codigo = c.Codigo
	),
	diferencas_agg AS (
		SELECT
			db.Thunders
			, db.UnidadeNegocio
			, db.Codigo
			, db.Entrega
			, db.DataFornecimento

			, DataHistorico_d0 = d0.DataHistorico
			, DataHistorico_d1 = d1.DataHistorico
		
			, db.PrincipalMudanca
			, db.PrincipalMudancaCurvaPLD
			, db.PrincipalMudancaPortfolio


			, Portfolio_d0
			, Portfolio_d1
			, Portfolio = COALESCE(d0.Portfolio, d1.Portfolio)
		
			, Submercado_d0
			, Submercado_d1
			, Submercado = COALESCE(d0.Submercado, d1.Submercado)
		
			, FonteEnergia_d0
			, FonteEnergia_d1
			, FonteEnergia = COALESCE(d0.FonteEnergia, d1.FonteEnergia)

			, TipoFlexibilidadePreco_d0
			, TipoFlexibilidadePreco_d1
			, TipoFlexibilidadePreco = COALESCE(d0.TipoFlexibilidadePreco, d1.TipoFlexibilidadePreco)
		

			, PrecoContratoRef_d1 = d1.PrecoContratoRef
			, PrecoContratoRef_d0 = d0.PrecoContratoRef

			, PrecoComparacao_d1 = d1.PrecoComparacao
			, PrecoComparacao_d0 = d0.PrecoComparacao

			, VolumeFinal_MWh_d1 = d1.VolumeFinal_MWh
			, VolumeFinal_MWh_d0 = d0.VolumeFinal_MWh

			, VolumeNet_MWh_d0 = (CASE d0.NaturezaOperacao WHEN 'Compra' THEN 1 ELSE -1 END) * d0.VolumeFinal_MWh
			, VolumeNet_MWh_d1 = (CASE d1.NaturezaOperacao WHEN 'Compra' THEN 1 ELSE -1 END) * d1.VolumeFinal_MWh
		
			, Resultado_d0 = d0.Resultado
			, Resultado_d1 = d1.Resultado

		
			, Resultado2_d0 = d0.Resultado2
			, Resultado2_d1 = d1.Resultado2



			, Resultado3_d0 = d0.Resultado3
			, Resultado3_d1 = d1.Resultado3

		
			--###################################################################################
			, DIFF_Resultado3 =
			db.MultiplicadorVariaveisNulas*
			(ISNULL(d0.Resultado3, 0) - ISNULL(d1.Resultado3, 0))

		
			--###################################################################################
			, DIFF_Resultado3_Volume =
			db.MultiplicadorVariaveisNulas*
			db.MultiplicadorBoletaInativada*
			db.MultiplicadorRegistroCriadoDeletado*
			(
				CASE d0.NaturezaOperacao
					WHEN 'Compra' Then -1
					WHEN 'Venda' Then 1
				END
			)*
			(
				(ISNULL(d0.VolumeFinal_MWh,0) - ISNULL(d1.VolumeFinal_MWh,0))
					* ISNULL(
						d1.PrecoContratoRef - d1.PrecoComparacao,0
					)
			)
		
		
			--###################################################################################
			, DIFF_Resultado3_PrecoContrato =
			db.MultiplicadorVariaveisNulas*
			db.MultiplicadorBoletaInativada*
			db.MultiplicadorRegistroCriadoDeletado*
			db.MultiplicadorTipoFlexibilidadePreco*
			(
				CASE d0.NaturezaOperacao
					WHEN 'Compra' Then -1
					WHEN 'Venda' Then 1
				END
			)*((ISNULL(d0.PrecoContratoRef,0) - ISNULL(d1.PrecoContratoRef,0))
				* d0.VolumeFinal_MWh)
		

			--###################################################################################
			, DIFF_Resultado3_CurvaTotal =
			-db.MultiplicadorVariaveisNulas*
			db.MultiplicadorBoletaInativada*
			db.MultiplicadorRegistroCriadoDeletado*
			(
				CASE d0.NaturezaOperacao
					WHEN 'Compra' Then -1
					WHEN 'Venda' Then 1
				END
			)*(d0.VolumeFinal_MWh
				* (ISNULL(d0.PrecoComparacao,0) - ISNULL(d1.PrecoComparacao,0)))

		
			--###################################################################################
			, DIFF_Resultado3_CurvaRef =
			-db.MultiplicadorVariaveisNulas*
			db.MultiplicadorBoletaInativada*
			db.MultiplicadorRegistroCriadoDeletado*
			(
				CASE d0.NaturezaOperacao
					WHEN 'Compra' Then -1
					WHEN 'Venda' Then 1
				END
			)*(d0.VolumeFinal_MWh
				* (ISNULL(d0.PrecoComparacao_Ref,0) - ISNULL(d1.PrecoComparacao_Ref,0)))
		

			--###################################################################################
			, DIFF_Resultado3_CurvaAgio =
			-db.MultiplicadorVariaveisNulas*
			db.MultiplicadorBoletaInativada*
			db.MultiplicadorRegistroCriadoDeletado*
			(
				CASE d0.NaturezaOperacao
					WHEN 'Compra' Then -1
					WHEN 'Venda' Then 1
				END
			)*(d0.VolumeFinal_MWh
				* (ISNULL(d0.PrecoComparacao_Agio,0) - ISNULL(d1.PrecoComparacao_Agio,0)))
		
		
			--###################################################################################
			, DIFF_Resultado3_CurvaSubmercado =
			-db.MultiplicadorVariaveisNulas*
			db.MultiplicadorBoletaInativada*
			db.MultiplicadorRegistroCriadoDeletado*
			db.MultiplicadorMudancaSubmercado*
			(
				CASE d0.NaturezaOperacao
					WHEN 'Compra' Then -1
					WHEN 'Venda' Then 1
				END
			)*(d0.VolumeFinal_MWh
				* (ISNULL(d0.PrecoComparacao_Spread_Submercado,0) - ISNULL(d1.PrecoComparacao_Spread_Submercado,0)))
		
		
			--###################################################################################
			, DIFF_Resultado3_CurvaFonteEnergia =
			-db.MultiplicadorVariaveisNulas*
			db.MultiplicadorBoletaInativada*
			db.MultiplicadorRegistroCriadoDeletado*
			db.MultiplicadorMudancaFonteEnergia*
			(
				CASE d0.NaturezaOperacao
					WHEN 'Compra' Then -1
					WHEN 'Venda' Then 1
				END
			)*(d0.VolumeFinal_MWh
				* (ISNULL(d0.PrecoComparacao_Spread_fonte_energia,0) - ISNULL(d1.PrecoComparacao_Spread_fonte_energia,0)))


			--###################################################################################
			, DIFF_Resultado3_BoletaInativada =
			db.MultiplicadorVariaveisNulas*
			(CASE WHEN db.MultiplicadorBoletaInativada = 0 THEN 1 ELSE 0 END) *(
			ISNULL(d0.Resultado3, 0) - ISNULL(d1.Resultado3, 0))
		

			--###################################################################################
			, DIFF_Resultado3_RegistroCriadoDeletado =
			db.MultiplicadorVariaveisNulas*
			(CASE WHEN db.MultiplicadorRegistroCriadoDeletado = 0 THEN 1 ELSE 0 END) *(
			ISNULL(d0.Resultado3, 0) - ISNULL(d1.Resultado3, 0))


			--###################################################################################
			, DIFF_Resultado3_MudancaSubmercado = 
			-db.MultiplicadorVariaveisNulas*
			db.MultiplicadorBoletaInativada*
			db.MultiplicadorRegistroCriadoDeletado*
			(CASE WHEN MultiplicadorMudancaSubmercado = 0 THEN 1 ELSE 0 END)*
			(
				CASE d0.NaturezaOperacao
					WHEN 'Compra' Then -1
					WHEN 'Venda' Then 1
				END
			)*(d0.VolumeFinal_MWh
				* (ISNULL(d0.PrecoComparacao_Spread_Submercado,0) - ISNULL(d1.PrecoComparacao_Spread_Submercado,0)))


			--###################################################################################
			, DIFF_Resultado3_MudancaFonteEnergia = 
			-db.MultiplicadorVariaveisNulas*
			db.MultiplicadorBoletaInativada*
			db.MultiplicadorRegistroCriadoDeletado*
			(CASE WHEN db.MultiplicadorMudancaFonteEnergia = 0 THEN 1 ELSE 0 END)*
			(
				CASE d0.NaturezaOperacao
					WHEN 'Compra' Then -1
					WHEN 'Venda' Then 1
				END
			)*(d0.VolumeFinal_MWh
				* (ISNULL(d0.PrecoComparacao_Spread_fonte_energia,0) - ISNULL(d1.PrecoComparacao_Spread_fonte_energia,0)))
		

			--###################################################################################
			, DIFF_Resultado3_MudancaTipoFlexibilidadePreco = 
			db.MultiplicadorVariaveisNulas*
			db.MultiplicadorBoletaInativada*
			db.MultiplicadorRegistroCriadoDeletado*
			(CASE WHEN db.MultiplicadorTipoFlexibilidadePreco = 0 THEN 1 ELSE 0 END)*
			(
				CASE d0.NaturezaOperacao
					WHEN 'Compra' Then -1
					WHEN 'Venda' Then 1
				END
			)*((ISNULL(d0.PrecoContratoRef,0) - ISNULL(d1.PrecoContratoRef,0))
				* d0.VolumeFinal_MWh)

		
			--###################################################################################
			, db.DIFF_Portfolio
			, db.DIFF_Classificacao
			, db.DIFF_portfolios
			, db.DIFF_BoletaAtiva
			, db.DIFF_TipoOperacao
			, db.DIFF_NaturezaOperacao
			, db.DIFF_Submercado
			, db.DIFF_FonteEnergia
			, db.DIFF_FlexibilidadePreco
			, db.DIFF_TipoFlexibilidadePreco
			, db.DIFF_VolumeFinal_MWh
			, db.DIFF_VolumeNet_MWh
			, db.DIFF_PrecoContrato
			, db.DIFF_PrecoFinal
			, db.DIFF_TetoPreco
			, db.DIFF_PisoPreco
			, db.DIFF_Spread
			, db.DIFF_PrecoPLDRef
			, db.DIFF_PrecoFuturoRef
			, db.DIFF_V_Curva_referencia
			, db.DIFF_V_Curva_Agio
			, db.DIFF_V_Curva_Spread_Submercado
			, db.DIFF_V_Curva_spread_fonte_energia
			, db.DIFF_T_PLD_Preco
			, db.DIFF_PrecoComparacao_Ref
			, db.DIFF_PrecoComparacao_Agio
			, db.DIFF_PrecoComparacao_Spread_Submercado
			, db.DIFF_PrecoComparacao_Spread_fonte_energia
			, db.DIFF_PrecoContratoRef
			, db.DIFF_PrecoComparacao

		FROM diff_registros db
		LEFT JOIN Modelo.dbo.proc_POC_Historico_resultado_d0 d0
			ON d0.Thunders = db.Thunders
			AND d0.UnidadeNegocio = db.UnidadeNegocio
			AND d0.Codigo = db.Codigo
			AND d0.Entrega = db.Entrega
			AND d0.DataFornecimento = db.DataFornecimento
			AND d0.BoletaAtiva = 1
		LEFT JOIN Modelo.dbo.proc_POC_Historico_resultado_d1 d1
			ON d1.Thunders = db.Thunders
			AND d1.UnidadeNegocio = db.UnidadeNegocio
			AND d1.Codigo = db.Codigo
			AND d1.Entrega = db.Entrega
			AND d1.DataFornecimento = db.DataFornecimento
			AND d1.BoletaAtiva = 1
		WHERE
			d0.BoletaAtiva = 1
			OR d1.BoletaAtiva = 1

	),
	final AS (
		SELECT
			DataHistorico_d0 = CAST(@data_historico_d0 AS DATE)
			, DataHistorico_d1 = CAST(@data_historico_d1 AS DATE)
			, d.Thunders
			, d.UnidadeNegocio
			, d.Codigo
			, d.Entrega
			, d.DataFornecimento
			, Thunders_UnidadeNegocio_Codigo = d.Thunders + '_' + d.UnidadeNegocio + '_' + d.Codigo
			, Thunders_UnidadeNegocio_Codigo_Entrega = d.Thunders + '_' + d.UnidadeNegocio + '_' + d.Codigo + '_' + d.Entrega
			, d.PrincipalMudanca
			, d.PrincipalMudancaCurvaPLD
			, d.PrincipalMudancaPortfolio 

			-- erro

			, d.Portfolio_d0
			, d.Portfolio_d1
			, d.Portfolio

			, d.Submercado_d0
			, d.Submercado_d1
			, d.Submercado
		
			, d.FonteEnergia_d0
			, d.FonteEnergia_d1
			, d.FonteEnergia

			, d.TipoFlexibilidadePreco_d0
			, d.TipoFlexibilidadePreco_d1
			, d.TipoFlexibilidadePreco
		
			, d.PrecoContratoRef_d1
			, d.PrecoContratoRef_d0

			, d.PrecoComparacao_d1
			, d.PrecoComparacao_d0

			, d.VolumeFinal_MWh_d1
			, d.VolumeFinal_MWh_d0

			, d.VolumeNet_MWh_d1
			, d.VolumeNet_MWh_d0
		
			, d.Resultado_d0
			, d.Resultado_d1

		
			, d.Resultado2_d0
			, d.Resultado2_d1

			, d.Resultado3_d0
			, d.Resultado3_d1

			, d.DIFF_Resultado3
			, d.DIFF_Resultado3_Volume
			, d.DIFF_Resultado3_PrecoContrato
			, d.DIFF_Resultado3_CurvaTotal
			, d.DIFF_Resultado3_CurvaRef
			, d.DIFF_Resultado3_CurvaAgio
			, d.DIFF_Resultado3_CurvaSubmercado
			, d.DIFF_Resultado3_CurvaFonteEnergia
			, d.DIFF_Resultado3_BoletaInativada
			, d.DIFF_Resultado3_RegistroCriadoDeletado
			, d.DIFF_Resultado3_MudancaSubmercado
			, d.DIFF_Resultado3_MudancaFonteEnergia
			, d.DIFF_Resultado3_MudancaTipoFlexibilidadePreco

			-- para simplificar estou usando a data do portfolio d+0 como referencia, mas o correto seria ter um tipo para cada uma das datas
			, CASE 
				WHEN d.DataFornecimento < CONVERT(DATE, (SELECT TOP 1 DataHistorico FROM Modelo.dbo.proc_POC_Historico_portfolio_d0)) THEN 'REALIZADO'
				WHEN d.DataFornecimento <= DATEADD(dd,360, CONVERT(DATE, (SELECT TOP 1 DataHistorico FROM Modelo.dbo.proc_POC_Historico_portfolio_d0))) THEN 'CIRCULANTE'
				ELSE 'NÃO CIRCULANTE'
			END AS [Circulante/Não Circulante]



			, [Taxa Desconto VPL D+0] = v0.taxa_desconto_vpl
			, [Lucro d+0] = CASE WHEN d.Resultado_d0 >= 0 THEN d.Resultado_d0 ELSE 0 END
			, [Prejuízo d+0] = CASE WHEN d.Resultado_d0 < 0 THEN d.Resultado_d0 ELSE 0 END
			, [MTM Ativo d+0] = CASE WHEN d.Resultado_d0 >= 0 THEN d.Resultado_d0 ELSE 0 END
			, [MTM Passivo d+0] = CASE WHEN d.Resultado_d0 < 0 THEN d.Resultado_d0 ELSE 0 END
			, [MTM Ativo VPL d+0] = CASE WHEN d.Resultado_d0 >= 0 THEN d.Resultado_d0 * v0.taxa_desconto_vpl ELSE 0 END
			, [MTM Passivo VPL d+0] = CASE WHEN d.Resultado_d0 < 0 THEN d.Resultado_d0 * v0.taxa_desconto_vpl ELSE 0 END
		
			, [Resultado d+0] = d.Resultado_d0
			, [Resultado VPL D+0] = d.Resultado_d0 * v0.taxa_desconto_vpl
			, [Resultado PIS/COFINS d+0] = d.Resultado_d0 * v0.taxa_desconto_vpl * (-0.0925)


			, [Taxa Desconto VPL D-1] = v1.taxa_desconto_vpl

			, [Lucro d-1] = CASE WHEN d.Resultado_d1 >= 0 THEN d.Resultado_d1 ELSE 0 END
			, [Prejuízo d-1] = CASE WHEN d.Resultado_d1 < 0 THEN d.Resultado_d1 ELSE 0 END
			, [MTM Ativo d-1] = CASE WHEN d.Resultado_d1 >= 0 THEN d.Resultado_d1 ELSE 0 END
			, [MTM Passivo d-1] = CASE WHEN d.Resultado_d1 < 0 THEN d.Resultado_d1 ELSE 0 END
			, [MTM Ativo VPL d-1] = CASE WHEN d.Resultado_d1 >= 0 THEN d.Resultado_d1 * v1.taxa_desconto_vpl ELSE 0 END
			, [MTM Passivo VPL d-1] = CASE WHEN d.Resultado_d1 < 0 THEN d.Resultado_d1 * v1.taxa_desconto_vpl ELSE 0 END

			, [Resultado d-1] = d.Resultado_d1
			, [Resultado VPL D-1] = d.Resultado_d1 * v1.taxa_desconto_vpl
			, [Resultado PIS/COFINS d-1] = d.Resultado_d1 * v1.taxa_desconto_vpl * (-0.0925)

			, d.DIFF_Portfolio
			, d.DIFF_Classificacao
			, d.DIFF_portfolios
			, d.DIFF_BoletaAtiva
			, d.DIFF_TipoOperacao
			, d.DIFF_NaturezaOperacao
			, d.DIFF_Submercado
			, d.DIFF_FonteEnergia
			, d.DIFF_FlexibilidadePreco
			, d.DIFF_TipoFlexibilidadePreco
			, d.DIFF_VolumeFinal_MWh
			, d.DIFF_VolumeNet_MWh
			, d.DIFF_PrecoContrato
			, d.DIFF_PrecoFinal
			, d.DIFF_TetoPreco
			, d.DIFF_PisoPreco
			, d.DIFF_Spread
			, d.DIFF_PrecoPLDRef
			, d.DIFF_PrecoFuturoRef
			, d.DIFF_V_Curva_referencia
			, d.DIFF_V_Curva_Agio
			, d.DIFF_V_Curva_Spread_Submercado
			, d.DIFF_V_Curva_spread_fonte_energia
			, d.DIFF_T_PLD_Preco
			, d.DIFF_PrecoComparacao_Ref
			, d.DIFF_PrecoComparacao_Agio
			, d.DIFF_PrecoComparacao_Spread_Submercado
			, d.DIFF_PrecoComparacao_Spread_fonte_energia
			, d.DIFF_PrecoContratoRef
			, d.DIFF_PrecoComparacao
		FROM diferencas_agg d
		LEFT JOIN Modelo.dbo.proc_POC_Historico_vpl_d0 v0
			ON d.DataFornecimento = v0.datafornecimento
		LEFT JOIN Modelo.dbo.proc_POC_Historico_vpl_d1 v1
			ON d.DataFornecimento = v1.datafornecimento
	)
	SELECT *
	INTO Modelo.dbo.proc_POC_Historico_diff_BI
	FROM final;

END
