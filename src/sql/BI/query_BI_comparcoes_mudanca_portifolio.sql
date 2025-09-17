-- Bases de DIFF_registros

DECLARE @data_historico_d0 AS DATETIME2 = (SELECT TOP 1 DAtaHistorico FROM Modelo.dbo.proc_POC_Historico_portfolio_d0);
DECLARE @data_historico_d1 AS DATETIME2 = (SELECT TOP 1 DAtaHistorico FROM Modelo.dbo.proc_POC_Historico_portfolio_d1);


DROP TABLE IF EXISTS #DIFF_registros;
with base as (
select
	Thunders = ISNULL(d_0.Thunders, d_1.Thunders)
	, UnidadeNegocio = ISNULL(d_0.UnidadeNegocio, d_1.UnidadeNegocio)
	, Codigo = ISNULL(d_0.Codigo, d_1.Codigo)
	, Entrega = ISNULL(d_0.Entrega, d_1.Entrega)
	, DataFornecimento = ISNULL(d_0.DataFornecimento, d_1.DataFornecimento)
	, Mudanca = CASE
		WHEN d_1.Thunders IS NULL THEN 'Data Fornecimento Criada'
		WHEN d_0.Thunders IS NULL THEN 'Data Fornecimento Deletada'
	END
from Modelo.dbo.proc_POC_Historico_portfolio_d0 d_0
full outer join Modelo.dbo.proc_POC_Historico_portfolio_d1 d_1
on 
	d_0.Thunders = d_1.Thunders
	AND d_0.UnidadeNegocio = d_1.UnidadeNegocio
	AND d_0.Codigo = d_1.Codigo
	AND d_0.Entrega = d_1.Entrega
	AND d_0.DataFornecimento = d_1.DataFornecimento
),
entregas as (
	select
	Thunders = ISNULL(d_0.Thunders, d_1.Thunders)
	, UnidadeNegocio = ISNULL(d_0.UnidadeNegocio, d_1.UnidadeNegocio)
	, Codigo = ISNULL(d_0.Codigo, d_1.Codigo)
	, Entrega = ISNULL(d_0.Entrega, d_1.Entrega)
	, Mudanca = CASE
		WHEN d_1.Thunders IS NULL THEN 'Entrega Criada'
		WHEN d_0.Thunders IS NULL THEN 'Entrega Deletada'
	END
from (
	select distinct
			Thunders,
			UnidadeNegocio,
			Codigo,
			Entrega
		from Modelo.dbo.proc_POC_Historico_portfolio_d0
	) d_0
	full outer join (
		select distinct
			Thunders,
			UnidadeNegocio,
			Codigo,
			Entrega
		from Modelo.dbo.proc_POC_Historico_portfolio_d1
	) d_1
	on 
		d_0.Thunders = d_1.Thunders
		AND d_0.UnidadeNegocio = d_1.UnidadeNegocio
		AND d_0.Codigo = d_1.Codigo
		AND d_0.Entrega = d_1.Entrega
),
boletas as (
	select
		Thunders = ISNULL(d_0.Thunders, d_1.Thunders)
		, UnidadeNegocio = ISNULL(d_0.UnidadeNegocio, d_1.UnidadeNegocio)
		, Codigo = ISNULL(d_0.Codigo, d_1.Codigo)
		, Mudanca = CASE
			WHEN d_1.Thunders IS NULL THEN 'Boleta Criada'
			WHEN d_0.Thunders IS NULL THEN 'Boleta Deletada'
		END
	from (
		select distinct
			Thunders,
			UnidadeNegocio,
			Codigo
		from Modelo.dbo.proc_POC_Historico_portfolio_d0
	) d_0
	full outer join (
		select distinct
			Thunders,
			UnidadeNegocio,
			Codigo
		from Modelo.dbo.proc_POC_Historico_portfolio_d1
	) d_1
	on 
		d_0.Thunders = d_1.Thunders
		AND d_0.UnidadeNegocio = d_1.UnidadeNegocio
		AND d_0.Codigo = d_1.Codigo
)
select
	b.Thunders
	, b.UnidadeNegocio
	, b.Codigo
	, b.Entrega
	, b.DataFornecimento
	, Mudanca = ISNULL(
		CAST(c.Mudanca AS VARCHAR(255)),
		ISNULL(
			CAST(e.Mudanca AS VARCHAR(255)),
			b.Mudanca
		)
	)

into #DIFF_registros
from base b
left join entregas e
on b.Thunders = e.Thunders
		AND b.UnidadeNegocio = e.UnidadeNegocio
		AND b.Codigo = e.Codigo
		AND b.Entrega = e.Entrega
left join boletas c
on b.Thunders = c.Thunders
		AND b.UnidadeNegocio = c.UnidadeNegocio
		AND b.Codigo = c.Codigo;



DROP TABLE IF EXISTS #DIFF_Agg;
with d0 as (
	select
		A.DataHistorico
		, A.Codigo
		, A.Entrega
		, A.DataFornecimento
		, A.isTrading
		, A.isServices
		, A.isGeneration
		, A.Thunders
		, A.EmpresaResponsavel
		, A.TipoNegocio
		, A.Classificacao
		, A.portfolios
		--, TipoContrato = 'Físico'
		, A.BoletaAtiva
		, A.UnidadeNegocio 
		, A.TipoOperacao
		, A.NaturezaOperacao
		, A.Submercado
		, A.FonteEnergia
		, A.FlexibilidadePreco
		, (CASE	WHEN A.FlexibilidadePreco = 'Fixo' THEN A.FlexibilidadePreco
									WHEN A.FlexibilidadePreco = 'Variável' THEN (CASE WHEN A.TetoPreco is null AND A.PisoPreco is null THEN A.FlexibilidadePreco --Variavel normal
																					 WHEN A.TetoPreco > 0 AND A.PisoPreco > 0 THEN 'Collar'
																					 WHEN A.TetoPreco > 0 OR A.PisoPreco > 0 THEN 'Opção'												 
																				  END )
										END) AS TipoFlexibilidadePreco
		, CASE WHEN A.Thunders = 'Safira' THEN  (CASE
                                                                    WHEN A.Classificacao = 'SPOT MESA'
                                                                        or A.Classificacao = 'GIRO RAPIDO' 
                                                                        or A.Classificacao = 'ESTRATEGIA MESA'  
                                                                        or A.Classificacao = 'Comercial Mesa'
                                                                        or A.Classificacao = 'Varejo'
                                                                        or A.Classificacao = 'Corporate' THEN 'Mesa'
                                                                    WHEN A.Classificacao = 'OP PF'  THEN 'PF_1'
                                                                    WHEN A.Classificacao = 'Varejo_Mesa'  THEN A.Classificacao
                                                                    WHEN A.Classificacao = 'Corporate_Mesa'  THEN A.Classificacao
                                                                    WHEN A.Classificacao = 'Operações_Estruturadas'  THEN 'Sfr'
                                                                    WHEN A.Classificacao = 'Manaus_Mesa'  THEN A.Classificacao
                                                                    WHEN A.Classificacao = 'Fuga' THEN A.Classificacao
                                                                    Else 'Sfr'
                                                            END)
                              WHEN Thunders = 'Comercial' THEN  (CASE
                                                                            WHEN A.Classificacao = 'Indra'  THEN 'Comercial_Indra'
                                                                            WHEN A.Classificacao = 'Safira'  THEN 'Pre_Comercial_Safira'
                                                                            WHEN A.Classificacao = 'Próprio'  THEN 'Comercial_Proprio'
                                                                            WHEN A.Classificacao = 'Sfr2'  THEN 'Sfr2'
                                                                            WHEN A.Classificacao = 'Mesa2'  THEN 'Mesa2'
                                                                            WHEN A.Classificacao = 'Varejista'  THEN 'Varejista'
                                                                            Else 'Pre_Comercial_Safira'
                                                                    END)
                              WHEN Thunders = 'Indra' THEN  (CASE
                                                            WHEN A.classificacao = 'Curto Prazo' OR  A.Classificacao = 'Longo Prazo'  THEN 'Trading'
                                                            WHEN A.classificacao = 'Flow'  THEN A.Classificacao
                                                            WHEN A.classificacao = 'Clientes'  THEN A.Classificacao
                                                            WHEN A.classificacao = 'Estruturadas'  THEN A.Classificacao
                                                            WHEN A.classificacao = 'op PF'  THEN 'PF_2'
                                                            Else 'Trading'
                                                    END     )
                        END as Portfolio
		, A.PrecoContrato -- Metrica
		, A.PrecoFinal -- Metrica
		, A.TetoPreco -- Metrica
		, A.PisoPreco -- Metrica
		, A.Spread -- Metrica
		, A.VolumeFinal_MWh -- Metrica
		, Book.Book.fn_Resultado(
			A.PrecoFinal,
			V_Curva.referencia + V_Curva.Agio + V_Curva.Spread_Submercado + V_Curva.spread_fonte_energia,
			T_PLD.Preco,
			A.TetoPreco,
			A.PisoPreco,
			V_Curva.referencia,
			A.Spread,
			A.NaturezaOperacao,
			A.FlexibilidadePreco,
			A.VolumeFinal_MWh
		) AS Resultado
		, ISNULL(V_Curva.referencia, T_PLD.Preco) AS PrecoPLDRef
		, ISNULL(
			V_Curva.referencia + V_Curva.Agio + V_Curva.Spread_Submercado + V_Curva.spread_fonte_energia,
			T_PLD.Preco
		) AS PrecoFuturoRef
		, V_Curva.referencia AS V_Curva_referencia
		, V_Curva.Agio AS V_Curva_Agio
		, V_Curva.Spread_Submercado AS V_Curva_Spread_Submercado
		, V_Curva.spread_fonte_energia AS V_Curva_spread_fonte_energia
		, T_PLD.Preco AS T_PLD_Preco

	from Modelo.dbo.proc_POC_Historico_portfolio_d0 A
	LEFT JOIN Modelo.dbo.proc_POC_Historico_PLD_d0 T_PLD
		ON A.DataFornecimento = T_PLD.[Data]
			AND A.Submercado = T_PLD.Submercado
	LEFT JOIN Modelo.dbo.proc_POC_Historico_Curva_d0 V_Curva 
		ON A.DataFornecimento = V_Curva.data_fwd
			AND A.Submercado = V_Curva.Submercado
			AND A.FonteEnergia = V_Curva.Fonte_Energia
	WHERE YEAR(DataFornecimento) > 2022
),
d0_t1 as (
	select
		*
		, CASE TipoFlexibilidadePreco
			WHEN 'Fixo' THEN PrecoFuturoRef
			WHEN 'Variável' THEN PrecoFuturoRef
			WHEN 'Collar' THEN PrecoPLDRef -- errado
			WHEN 'Opção' THEN PrecoFuturoRef
		END AS PrecoComparacao


		, CASE TipoFlexibilidadePreco
			WHEN 'Fixo' THEN ISNULL(V_Curva_referencia, T_PLD_Preco)
			WHEN 'Variável' THEN ISNULL(V_Curva_referencia, T_PLD_Preco)
			WHEN 'Collar' THEN PrecoPLDRef -- errado
			WHEN 'Opção' THEN ISNULL(V_Curva_referencia, T_PLD_Preco)
		END AS PrecoComparacao_Ref

		, CASE TipoFlexibilidadePreco
			WHEN 'Fixo' THEN ISNULL(V_Curva_Agio, 0)
			WHEN 'Variável' THEN ISNULL(V_Curva_Agio, 0)
			WHEN 'Collar' THEN 0 -- errado
			WHEN 'Opção' THEN ISNULL(V_Curva_Agio, 0)
		END AS PrecoComparacao_Agio

		, CASE TipoFlexibilidadePreco
			WHEN 'Fixo' THEN ISNULL(V_Curva_Spread_Submercado, 0)
			WHEN 'Variável' THEN ISNULL(V_Curva_Spread_Submercado, 0)
			WHEN 'Collar' THEN 0 -- errado
			WHEN 'Opção' THEN ISNULL(V_Curva_Spread_Submercado, 0)
		END AS PrecoComparacao_Spread_Submercado

		, CASE TipoFlexibilidadePreco
			WHEN 'Fixo' THEN ISNULL(V_Curva_spread_fonte_energia, 0)
			WHEN 'Variável' THEN ISNULL(V_Curva_spread_fonte_energia, 0)
			WHEN 'Collar' THEN 0 -- errado
			WHEN 'Opção' THEN ISNULL(V_Curva_spread_fonte_energia, 0)
		END AS PrecoComparacao_Spread_fonte_energia


		, CASE TipoFlexibilidadePreco
			WHEN 'Fixo' THEN PrecoFinal
			WHEN 'Variável' THEN PrecoPLDRef + Spread
			WHEN 'Collar' THEN
				CASE
					WHEN PrecoFuturoRef >= TetoPreco THEN TetoPreco
					WHEN PrecoFuturoRef <= PisoPreco THEN PisoPreco
					ELSE PrecoPLDRef + Spread
				END
			WHEN 'Opção' THEN
				CASE
					WHEN TetoPreco > 0 THEN
						CASE
							WHEN PrecoPLDRef + Spread >= TetoPreco THEN TetoPreco
							ELSE PrecoPLDRef + Spread
						END
					WHEN PisoPreco > 0 THEN
						CASE
							WHEN PrecoPLDRef + Spread <= PisoPreco THEN PisoPreco
							ELSE PrecoPLDRef + Spread
						END
				END
		END AS PrecoContratoRef
	from d0
),
d0_t2 as (
	select *
	, (
		CASE NaturezaOperacao
			WHEN 'Compra' Then -1
			WHEN 'Venda' Then 1
		END
	) * ROUND(
		(VolumeFinal_MWh * (PrecoContratoRef - PrecoComparacao)),
		2
	) AS Resultado2
	, (
		CASE NaturezaOperacao
			WHEN 'Compra' Then -1
			WHEN 'Venda' Then 1
		END
	) * ROUND(
		(VolumeFinal_MWh * (PrecoContratoRef - (PrecoComparacao_Ref + PrecoComparacao_Agio + PrecoComparacao_Spread_Submercado + PrecoComparacao_Spread_fonte_energia))),
		2
	) AS Resultado3
	from d0_t1
),
d1 as (
	select
		A.DataHistorico
		, A.Codigo
		, A.Entrega
		, A.DataFornecimento
		, A.isTrading
		, A.isServices
		, A.isGeneration
		, A.Thunders
		, A.EmpresaResponsavel
		, A.TipoNegocio
		, A.Classificacao
		, A.portfolios
		--, TipoContrato = 'Físico'
		, A.BoletaAtiva
		, A.UnidadeNegocio 
		, A.TipoOperacao
		, A.NaturezaOperacao
		, A.Submercado
		, A.FonteEnergia
		, A.FlexibilidadePreco
		, (CASE	WHEN A.FlexibilidadePreco = 'Fixo' THEN A.FlexibilidadePreco
									WHEN A.FlexibilidadePreco = 'Variável' THEN (CASE WHEN A.TetoPreco is null AND A.PisoPreco is null THEN A.FlexibilidadePreco --Variavel normal
																					 WHEN A.TetoPreco > 0 AND A.PisoPreco > 0 THEN 'Collar'
																					 WHEN A.TetoPreco > 0 OR A.PisoPreco > 0 THEN 'Opção'												 
																				  END )
										END) AS TipoFlexibilidadePreco
		, CASE WHEN A.Thunders = 'Safira' THEN  (CASE
                                                                    WHEN A.Classificacao = 'SPOT MESA'
                                                                        or A.Classificacao = 'GIRO RAPIDO' 
                                                                        or A.Classificacao = 'ESTRATEGIA MESA'  
                                                                        or A.Classificacao = 'Comercial Mesa'
                                                                        or A.Classificacao = 'Varejo'
                                                                        or A.Classificacao = 'Corporate' THEN 'Mesa'
                                                                    WHEN A.Classificacao = 'OP PF'  THEN 'PF_1'
                                                                    WHEN A.Classificacao = 'Varejo_Mesa'  THEN A.Classificacao
                                                                    WHEN A.Classificacao = 'Corporate_Mesa'  THEN A.Classificacao
                                                                    WHEN A.Classificacao = 'Operações_Estruturadas'  THEN 'Sfr'
                                                                    WHEN A.Classificacao = 'Manaus_Mesa'  THEN A.Classificacao
                                                                    WHEN A.Classificacao = 'Fuga' THEN A.Classificacao
                                                                    Else 'Sfr'
                                                            END)
                              WHEN Thunders = 'Comercial' THEN  (CASE
                                                                            WHEN A.Classificacao = 'Indra'  THEN 'Comercial_Indra'
                                                                            WHEN A.Classificacao = 'Safira'  THEN 'Pre_Comercial_Safira'
                                                                            WHEN A.Classificacao = 'Próprio'  THEN 'Comercial_Proprio'
                                                                            WHEN A.Classificacao = 'Sfr2'  THEN 'Sfr2'
                                                                            WHEN A.Classificacao = 'Mesa2'  THEN 'Mesa2'
                                                                            WHEN A.Classificacao = 'Varejista'  THEN 'Varejista'
                                                                            Else 'Pre_Comercial_Safira'
                                                                    END)
                              WHEN Thunders = 'Indra' THEN  (CASE
                                                            WHEN A.classificacao = 'Curto Prazo' OR  A.Classificacao = 'Longo Prazo'  THEN 'Trading'
                                                            WHEN A.classificacao = 'Flow'  THEN A.Classificacao
                                                            WHEN A.classificacao = 'Clientes'  THEN A.Classificacao
                                                            WHEN A.classificacao = 'Estruturadas'  THEN A.Classificacao
                                                            WHEN A.classificacao = 'op PF'  THEN 'PF_2'
                                                            Else 'Trading'
                                                    END     )
                        END as Portfolio
		, A.PrecoContrato -- Metrica
		, A.PrecoFinal -- Metrica
		, A.TetoPreco -- Metrica
		, A.PisoPreco -- Metrica
		, A.Spread -- Metrica
		, A.VolumeFinal_MWh -- Metrica
		, Book.Book.fn_Resultado(
			A.PrecoFinal,
			V_Curva.referencia + V_Curva.Agio + V_Curva.Spread_Submercado + V_Curva.spread_fonte_energia,
			T_PLD.Preco,
			A.TetoPreco,
			A.PisoPreco,
			V_Curva.referencia,
			A.Spread,
			A.NaturezaOperacao,
			A.FlexibilidadePreco,
			A.VolumeFinal_MWh
		) AS Resultado
		, ISNULL(V_Curva.referencia, T_PLD.Preco) AS PrecoPLDRef
		, ISNULL(
			V_Curva.referencia + V_Curva.Agio + V_Curva.Spread_Submercado + V_Curva.spread_fonte_energia,
			T_PLD.Preco
		) AS PrecoFuturoRef
		, V_Curva.referencia AS V_Curva_referencia
		, V_Curva.Agio AS V_Curva_Agio
		, V_Curva.Spread_Submercado AS V_Curva_Spread_Submercado
		, V_Curva.spread_fonte_energia AS V_Curva_spread_fonte_energia
		, T_PLD.Preco AS T_PLD_Preco

	from Modelo.dbo.proc_POC_Historico_portfolio_d1 A
	LEFT JOIN Modelo.dbo.proc_POC_Historico_PLD_d1 T_PLD
		ON A.DataFornecimento = T_PLD.[Data]
			AND A.Submercado = T_PLD.Submercado
	LEFT JOIN Modelo.dbo.proc_POC_Historico_Curva_d1 V_Curva 
		ON A.DataFornecimento = V_Curva.data_fwd
			AND A.Submercado = V_Curva.Submercado
			AND A.FonteEnergia = V_Curva.Fonte_Energia
	WHERE YEAR(DataFornecimento) > 2022
),
d1_t1 as (
	select
		*
		, CASE TipoFlexibilidadePreco
			WHEN 'Fixo' THEN PrecoFuturoRef
			WHEN 'Variável' THEN PrecoFuturoRef
			WHEN 'Collar' THEN PrecoPLDRef -- errado
			WHEN 'Opção' THEN PrecoFuturoRef
		END AS PrecoComparacao


		, CASE TipoFlexibilidadePreco
			WHEN 'Fixo' THEN ISNULL(V_Curva_referencia, T_PLD_Preco)
			WHEN 'Variável' THEN ISNULL(V_Curva_referencia, T_PLD_Preco)
			WHEN 'Collar' THEN PrecoPLDRef -- errado
			WHEN 'Opção' THEN ISNULL(V_Curva_referencia, T_PLD_Preco)
		END AS PrecoComparacao_Ref

		, CASE TipoFlexibilidadePreco
			WHEN 'Fixo' THEN ISNULL(V_Curva_Agio, 0)
			WHEN 'Variável' THEN ISNULL(V_Curva_Agio, 0)
			WHEN 'Collar' THEN 0 -- errado
			WHEN 'Opção' THEN ISNULL(V_Curva_Agio, 0)
		END AS PrecoComparacao_Agio

		, CASE TipoFlexibilidadePreco
			WHEN 'Fixo' THEN ISNULL(V_Curva_Spread_Submercado, 0)
			WHEN 'Variável' THEN ISNULL(V_Curva_Spread_Submercado, 0)
			WHEN 'Collar' THEN 0 -- errado
			WHEN 'Opção' THEN ISNULL(V_Curva_Spread_Submercado, 0)
		END AS PrecoComparacao_Spread_Submercado

		, CASE TipoFlexibilidadePreco
			WHEN 'Fixo' THEN ISNULL(V_Curva_spread_fonte_energia, 0)
			WHEN 'Variável' THEN ISNULL(V_Curva_spread_fonte_energia, 0)
			WHEN 'Collar' THEN 0 -- errado
			WHEN 'Opção' THEN ISNULL(V_Curva_spread_fonte_energia, 0)
		END AS PrecoComparacao_Spread_fonte_energia


		, CASE TipoFlexibilidadePreco
			WHEN 'Fixo' THEN PrecoFinal
			WHEN 'Variável' THEN PrecoPLDRef + Spread
			WHEN 'Collar' THEN
				CASE
					WHEN PrecoFuturoRef >= TetoPreco THEN TetoPreco
					WHEN PrecoFuturoRef <= PisoPreco THEN PisoPreco
					ELSE PrecoPLDRef + Spread
				END
			WHEN 'Opção' THEN
				CASE
					WHEN TetoPreco > 0 THEN
						CASE
							WHEN PrecoPLDRef + Spread >= TetoPreco THEN TetoPreco
							ELSE PrecoPLDRef + Spread
						END
					WHEN PisoPreco > 0 THEN
						CASE
							WHEN PrecoPLDRef + Spread <= PisoPreco THEN PisoPreco
							ELSE PrecoPLDRef + Spread
						END
				END
		END AS PrecoContratoRef
	from d1
),
d1_t2 as (
	select *
	, (
		CASE NaturezaOperacao
			WHEN 'Compra' Then -1
			WHEN 'Venda' Then 1
		END
	) * ROUND(
		(VolumeFinal_MWh * (PrecoContratoRef - PrecoComparacao)),
		2
	) AS Resultado2
	, (
		CASE NaturezaOperacao
			WHEN 'Compra' Then -1
			WHEN 'Venda' Then 1
		END
	) * ROUND(
		(VolumeFinal_MWh * (PrecoContratoRef - (PrecoComparacao_Ref + PrecoComparacao_Agio + PrecoComparacao_Spread_Submercado + PrecoComparacao_Spread_fonte_energia))),
		2
	) AS Resultado3
	from d1_t1
),
diferencas_boletas as (
	select
		Thunders = ISNULL(d_0.Thunders, d_1.Thunders)
		, UnidadeNegocio = ISNULL(d_0.UnidadeNegocio, d_1.UnidadeNegocio)
		, Codigo = ISNULL(d_0.Codigo, d_1.Codigo)
		, Entrega = ISNULL(d_0.Entrega, d_1.Entrega)
		, DataFornecimento = ISNULL(d_0.DataFornecimento, d_1.DataFornecimento)
		
		, MudancaLinha = CASE
			WHEN d_1.Thunders IS NULL THEN 'Registro Criado'
			WHEN d_0.Thunders IS NULL THEN 'Registro Deletado'
		END

		, DIFF_Portfolio =
			CASE
				WHEN ISNULL(CAST(d_1.Portfolio AS VARCHAR),'NULL') != ISNULL(CAST(d_0.Portfolio AS VARCHAR),'NULL')
				THEN ISNULL(CAST(d_1.Portfolio AS VARCHAR),'NULL') + ' -> ' + ISNULL(CAST(d_0.Portfolio AS VARCHAR),'NULL')
		END
		, DIFF_Classificacao =
			CASE
				WHEN ISNULL(CAST(d_1.Classificacao AS VARCHAR),'NULL') != ISNULL(CAST(d_0.Classificacao AS VARCHAR),'NULL')
				THEN ISNULL(CAST(d_1.Classificacao AS VARCHAR),'NULL') + ' -> ' + ISNULL(CAST(d_0.Classificacao AS VARCHAR),'NULL')
		END
		, DIFF_portfolios =
			CASE
				WHEN ISNULL(CAST(d_1.portfolios AS VARCHAR),'NULL') != ISNULL(CAST(d_0.portfolios AS VARCHAR),'NULL')
				THEN ISNULL(CAST(d_1.portfolios AS VARCHAR),'NULL') + ' -> ' + ISNULL(CAST(d_0.portfolios AS VARCHAR),'NULL')
		END

		, BoletaAtiva_d0 = d_0.BoletaAtiva
		, BoletaAtiva_d1 = d_1.BoletaAtiva
		, DIFF_BoletaAtiva =
			CASE
				WHEN ISNULL(CAST(d_1.BoletaAtiva AS VARCHAR),'NULL') != ISNULL(CAST(d_0.BoletaAtiva AS VARCHAR),'NULL')
				THEN ISNULL(CAST(d_1.BoletaAtiva AS VARCHAR),'NULL') + ' -> ' + ISNULL(CAST(d_0.BoletaAtiva AS VARCHAR),'NULL')
		END
		, DIFF_TipoOperacao = 
			CASE
				WHEN ISNULL(CAST(d_1.TipoOperacao AS VARCHAR),'NULL') != ISNULL(CAST(d_0.TipoOperacao AS VARCHAR),'NULL')
				THEN ISNULL(CAST(d_1.TipoOperacao AS VARCHAR),'NULL') + ' -> ' + ISNULL(CAST(d_0.TipoOperacao AS VARCHAR),'NULL')
		END
		, DIFF_NaturezaOperacao = 
			CASE
				WHEN ISNULL(CAST(d_1.NaturezaOperacao AS VARCHAR),'NULL') != ISNULL(CAST(d_0.NaturezaOperacao AS VARCHAR),'NULL')
				THEN ISNULL(CAST(d_1.NaturezaOperacao AS VARCHAR),'NULL') + ' -> ' + ISNULL(CAST(d_0.NaturezaOperacao AS VARCHAR),'NULL')
		END
		, DIFF_Submercado = 
			CASE
				WHEN ISNULL(CAST(d_1.Submercado AS VARCHAR),'NULL') != ISNULL(CAST(d_0.Submercado AS VARCHAR),'NULL')
				THEN ISNULL(CAST(d_1.Submercado AS VARCHAR),'NULL') + ' -> ' + ISNULL(CAST(d_0.Submercado AS VARCHAR),'NULL')
		END
		, DIFF_FonteEnergia = 
			CASE
				WHEN ISNULL(CAST(d_1.FonteEnergia AS VARCHAR),'NULL') != ISNULL(CAST(d_0.FonteEnergia AS VARCHAR),'NULL')
				THEN ISNULL(CAST(d_1.FonteEnergia AS VARCHAR),'NULL') + ' -> ' + ISNULL(CAST(d_0.FonteEnergia AS VARCHAR),'NULL')
		END
		, DIFF_FlexibilidadePreco = 
			CASE
				WHEN ISNULL(CAST(d_1.FlexibilidadePreco AS VARCHAR),'NULL') != ISNULL(CAST(d_0.FlexibilidadePreco AS VARCHAR),'NULL')
				THEN ISNULL(CAST(d_1.FlexibilidadePreco AS VARCHAR),'NULL') + ' -> ' + ISNULL(CAST(d_0.FlexibilidadePreco AS VARCHAR),'NULL')
		END
		, DIFF_TipoFlexibilidadePreco = 
			CASE
				WHEN ISNULL(CAST(d_1.TipoFlexibilidadePreco AS VARCHAR),'NULL') != ISNULL(CAST(d_0.TipoFlexibilidadePreco AS VARCHAR),'NULL')
				THEN ISNULL(CAST(d_1.TipoFlexibilidadePreco AS VARCHAR),'NULL') + ' -> ' + ISNULL(CAST(d_0.TipoFlexibilidadePreco AS VARCHAR),'NULL')
		END

		, DIFF_VolumeFinal_MWh = ISNULL(d_0.VolumeFinal_MWh,0) - ISNULL(d_1.VolumeFinal_MWh,0)
		, DIFF_PrecoContrato = ISNULL(d_0.PrecoContrato,0) - ISNULL(d_1.PrecoContrato,0)
		, DIFF_PrecoFinal = ISNULL(d_0.PrecoFinal,0) - ISNULL(d_1.PrecoFinal,0)
		, DIFF_TetoPreco = ISNULL(d_0.TetoPreco,0) - ISNULL(d_1.TetoPreco,0)
		, DIFF_PisoPreco = ISNULL(d_0.PisoPreco,0) - ISNULL(d_1.PisoPreco,0)
		, DIFF_Spread = ISNULL(d_0.Spread,0) - ISNULL(d_1.Spread,0)

		, VolumeFinal_MWh_d1 = d_1.VolumeFinal_MWh
		, VolumeFinal_MWh_d0 = d_0.VolumeFinal_MWh
		
		, PrecoContrato_d0 = d_0.PrecoContrato
		, PrecoContrato_d1 = d_1.PrecoContrato

		, PrecoFinal_d0 = d_0.PrecoFinal
		, PrecoFinal_d1 = d_1.PrecoFinal

		, TetoPreco_d0 = d_0.TetoPreco
		, TetoPreco_d1 = d_1.TetoPreco

		, PisoPreco_d0 = d_0.PisoPreco
		, PisoPreco_d1 = d_1.PisoPreco

		, Spread_d0 = d_0.Spread
		, Spread_d1 = d_1.Spread

		, Portfolio_d0 = d_0.Portfolio
		, Portfolio_d1 = d_1.Portfolio


		, DIFF_PrecoPLDRef = ISNULL(d_0.PrecoPLDRef,0) - ISNULL(d_1.PrecoPLDRef,0)
		, DIFF_PrecoFuturoRef = ISNULL(d_0.PrecoFuturoRef,0) - ISNULL(d_1.PrecoFuturoRef,0)
		, DIFF_V_Curva_referencia = ISNULL(d_0.V_Curva_referencia,0) - ISNULL(d_1.V_Curva_referencia,0)
		, DIFF_V_Curva_Agio = ISNULL(d_0.V_Curva_Agio,0) - ISNULL(d_1.V_Curva_Agio,0)
		, DIFF_V_Curva_Spread_Submercado = ISNULL(d_0.V_Curva_Spread_Submercado,0) - ISNULL(d_1.V_Curva_Spread_Submercado,0)
		, DIFF_V_Curva_spread_fonte_energia = ISNULL(d_0.V_Curva_spread_fonte_energia,0) - ISNULL(d_1.V_Curva_spread_fonte_energia,0)
		, DIFF_T_PLD_Preco = ISNULL(d_0.T_PLD_Preco,0) - ISNULL(d_1.T_PLD_Preco,0)
		, DIFF_PrecoComparacao = ISNULL(d_0.PrecoComparacao,0) - ISNULL(d_1.PrecoComparacao,0)
		, DIFF_PrecoComparacao_Ref = ISNULL(d_0.PrecoComparacao_Ref,0) - ISNULL(d_1.PrecoComparacao_Ref,0)
		, DIFF_PrecoComparacao_Agio = ISNULL(d_0.PrecoComparacao_Agio,0) - ISNULL(d_1.PrecoComparacao_Agio,0)
		, DIFF_PrecoComparacao_Spread_Submercado = ISNULL(d_0.PrecoComparacao_Spread_Submercado,0) - ISNULL(d_1.PrecoComparacao_Spread_Submercado,0)
		, DIFF_PrecoComparacao_Spread_fonte_energia = ISNULL(d_0.PrecoComparacao_Spread_fonte_energia,0) - ISNULL(d_1.PrecoComparacao_Spread_fonte_energia,0)
		, DIFF_PrecoContratoRef = ISNULL(d_0.PrecoContratoRef,0) - ISNULL(d_1.PrecoContratoRef,0)


		, PrecoContratoRef_d0 = d_0.PrecoContratoRef
		, PrecoContratoRef_d1 = d_1.PrecoContratoRef

		, PrecoComparacao_d0 = d_0.PrecoComparacao
		, PrecoComparacao_d1 = d_1.PrecoComparacao

		
		, PrecoComparacao_Ref_d0 = d_0.PrecoComparacao_Ref
		, PrecoComparacao_Ref_d1 = d_1.PrecoComparacao_Ref
		, PrecoComparacao_Agio_d0 = d_0.PrecoComparacao_Agio
		, PrecoComparacao_Agio_d1 = d_1.PrecoComparacao_Agio
		, PrecoComparacao_Spread_Submercado_d0 = d_0.PrecoComparacao_Spread_Submercado
		, PrecoComparacao_Spread_Submercado_d1 = d_1.PrecoComparacao_Spread_Submercado
		, PrecoComparacao_Spread_fonte_energia_d0 = d_0.PrecoComparacao_Spread_fonte_energia
		, PrecoComparacao_Spread_fonte_energia_d1 = d_1.PrecoComparacao_Spread_fonte_energia
		

		, Resultado_d0 = d_0.Resultado
		, Resultado_d1 = d_1.Resultado
		, Resultado2_d0 = d_0.Resultado2
		, Resultado2_d1 = d_1.Resultado2
		, Resultado3_d0 = d_0.Resultado3
		, Resultado3_d1 = d_1.Resultado3


	from d0_t2 d_0
	full outer join d1_t2 d_1
	on
		(d_0.Thunders = d_1.Thunders OR (d_0.Thunders IS NULL AND d_1.Thunders IS NULL))
		AND (d_0.UnidadeNegocio = d_1.UnidadeNegocio OR (d_0.UnidadeNegocio IS NULL AND d_1.UnidadeNegocio IS NULL))
		AND (d_0.Codigo = d_1.Codigo OR (d_0.Codigo IS NULL AND d_1.Codigo IS NULL))
		AND (d_0.Entrega = d_1.Entrega OR (d_0.Entrega IS NULL AND d_1.Entrega IS NULL))
		AND (d_0.DataFornecimento = d_1.DataFornecimento OR (d_0.DataFornecimento IS NULL AND d_1.DataFornecimento IS NULL))
		

),
diferencas_boletas_resumo AS (
	select
		d.Thunders
		, d.UnidadeNegocio
		, d.Codigo
		, d.Entrega
		, d.DataFornecimento
		, BoletaAtiva_d0
		, BoletaAtiva_d1
		
		, PrincipalMudanca = CASE
			WHEN m.Mudanca IS NOT NULL THEN m.Mudanca -- Criação ou deleção de registros
			WHEN DIFF_Portfolio IS NOT NULL THEN 'Portfólio'
			WHEN DIFF_BoletaAtiva = '0 -> 1' THEN 'Boleta Ativada'
			WHEN DIFF_BoletaAtiva = '1 -> 0' THEN 'Boleta Inativada'
			WHEN DIFF_VolumeFinal_MWh <> 0 AND VolumeFinal_MWh_d0 = 0 THEN 'Boleta Zerada'
			WHEN DIFF_VolumeFinal_MWh <> 0 AND VolumeFinal_MWh_d1 = 0 THEN 'Boleta Deszerada'
			WHEN DIFF_NaturezaOperacao IS NOT NULL THEN 'Natureza de Operação'
			WHEN DIFF_Submercado IS NOT NULL THEN 'Submercado'
			WHEN DIFF_FonteEnergia IS NOT NULL THEN 'Fonte de Energia'
			WHEN DIFF_FlexibilidadePreco IS NOT NULL THEN 'Flexibilidade de Preço'
			WHEN DIFF_TipoFlexibilidadePreco IS NOT NULL THEN 'Tipo Flexibilidade de Preço'
			WHEN DIFF_VolumeFinal_MWh <> 0 THEN 'Volume'
			WHEN DIFF_PrecoContrato <> 0 THEN 'Preço Contrato'
			WHEN DIFF_PrecoFinal <> 0 THEN 'Preço Final'
			WHEN (
				DIFF_PisoPreco <> 0
				or DIFF_PrecoContrato <> 0
				or DIFF_PrecoFinal <> 0
				or DIFF_Spread <> 0
				or DIFF_TetoPreco <> 0
				or DIFF_VolumeFinal_MWh <> 0
				--or DIFF_VolumeFinal_MWm <> 0
			) THEN 'Outros - Campo Numérico'
			WHEN (
				DIFF_Portfolio IS NOT NULL
				OR DIFF_Classificacao IS NOT NULL
				OR DIFF_portfolios IS NOT NULL
				OR DIFF_BoletaAtiva IS NOT NULL
				OR DIFF_TipoOperacao  IS NOT NULL 
				OR DIFF_NaturezaOperacao  IS NOT NULL
				OR DIFF_Submercado IS NOT NULL
				OR DIFF_FonteEnergia IS NOT NULL
				OR DIFF_FlexibilidadePreco IS NOT NULL
				OR DIFF_TipoFlexibilidadePreco IS NOT NULL
			) THEN 'Outros - Campo Categórico'
			WHEN (
				DIFF_PrecoPLDRef IS NOT NULL
				OR DIFF_PrecoFuturoRef IS NOT NULL
				OR DIFF_V_Curva_referencia IS NOT NULL
				OR DIFF_V_Curva_Agio IS NOT NULL
				OR DIFF_V_Curva_Spread_Submercado IS NOT NULL
				OR DIFF_V_Curva_spread_fonte_energia IS NOT NULL
				OR DIFF_T_PLD_Preco IS NOT NULL
			) THEN 'Curva/PLD'
		END

		, PrincipalMudancaPortfolio = CASE
			WHEN DIFF_VolumeFinal_MWh <> 0 THEN 'Volume'
			WHEN DIFF_PrecoContrato <> 0 THEN 'Preço Contrato'
			WHEN DIFF_PrecoFinal <> 0 THEN 'Preço Final'
			WHEN DIFF_TetoPreco <> 0 THEN 'Teto Preço'
			WHEN DIFF_PisoPreco <> 0 THEN 'Piso Preço'
			WHEN DIFF_Spread <> 0 THEN 'Spread'		
		END

		, PrincipalMudancaCurvaPLD = CASE
			WHEN DIFF_T_PLD_Preco <> 0 THEN 'Inclusão de PLD'
			WHEN DIFF_V_Curva_referencia <> 0 THEN 'Referência'
			WHEN DIFF_V_Curva_Spread_Submercado <> 0 THEN 'Submercado'
			WHEN DIFF_V_Curva_spread_fonte_energia <> 0 THEN 'Fonte de Energia'
			WHEN DIFF_V_Curva_Agio <> 0 THEN 'Agio'	
		END

		, Portfolio_d0
		, Portfolio_d1
		, DIFF_Portfolio
		, DIFF_Classificacao
		, DIFF_portfolios
		, DIFF_BoletaAtiva
		, DIFF_TipoOperacao
		, DIFF_NaturezaOperacao
		, DIFF_Submercado
		, DIFF_FonteEnergia
		, DIFF_FlexibilidadePreco
		, DIFF_TipoFlexibilidadePreco

		, VolumeFinal_MWh_d1
		, VolumeFinal_MWh_d0
		, DIFF_VolumeFinal_MWh
		
		, PrecoContrato_d0
		, PrecoContrato_d1
		, DIFF_PrecoContrato
		
		, PrecoFinal_d0
		, PrecoFinal_d1
		, DIFF_PrecoFinal
		
		, TetoPreco_d0
		, TetoPreco_d1
		, DIFF_TetoPreco

		, PisoPreco_d0
		, PisoPreco_d1
		, DIFF_PisoPreco

		, Spread_d0
		, Spread_d1
		, DIFF_Spread


		, DIFF_PrecoPLDRef
		, DIFF_PrecoFuturoRef
		, DIFF_V_Curva_referencia
		, DIFF_V_Curva_Agio
		, DIFF_V_Curva_Spread_Submercado
		, DIFF_V_Curva_spread_fonte_energia
		, DIFF_T_PLD_Preco
		
		, PrecoComparacao_Ref_d0
		, PrecoComparacao_Ref_d1
		, DIFF_PrecoComparacao_Ref
		
		, PrecoComparacao_Agio_d0
		, PrecoComparacao_Agio_d1
		, DIFF_PrecoComparacao_Agio
		
		, PrecoComparacao_Spread_Submercado_d0
		, PrecoComparacao_Spread_Submercado_d1
		, DIFF_PrecoComparacao_Spread_Submercado
		
		, PrecoComparacao_Spread_fonte_energia_d0
		, PrecoComparacao_Spread_fonte_energia_d1
		, DIFF_PrecoComparacao_Spread_fonte_energia
		
		
		, PrecoContratoRef_d0
		, PrecoContratoRef_d1
		, DIFF_PrecoContratoRef


		
		, PrecoComparacao_d0
		, PrecoComparacao_d1
		, DIFF_PrecoComparacao


		, Resultado_d0
		, Resultado_d1

		, Resultado2_d0
		, Resultado2_d1


		, Resultado3_d0
		, Resultado3_d1






	FROM diferencas_boletas d
	left join #DIFF_registros m
	on d.Thunders = m.Thunders
	AND d.UnidadeNegocio = m.UnidadeNegocio
	AND d.Codigo = m.Codigo
	AND d.Entrega = m.Entrega
	AND d.DataFornecimento = m.DataFornecimento

),
d0_mudancas as (
	select
		d_0.*
		, db.PrincipalMudanca
		, db.PrincipalMudancaCurvaPLD
		, db.PrincipalMudancaPortfolio
		, MultiplicadorResultado = (
			CASE NaturezaOperacao
				WHEN 'Compra' Then -1
				WHEN 'Venda' Then 1
			END
		)
		, MultiplicadorBoletaInativada = (
			CASE
				WHEN DIFF_BoletaAtiva = '0 -> 1' THEN 0
				WHEN DIFF_BoletaAtiva = '1 -> 0' THEN 0
				ELSE 1
			END
		)
		, MultiplicadorRegistroCriadoDeletado = (
			CASE
				WHEN PrincipalMudanca = 'Boleta Criada'
				OR PrincipalMudanca = 'Boleta Deletada'
				OR PrincipalMudanca = 'Entrega Criada'
				OR PrincipalMudanca = 'Entrega Deletada'
				OR PrincipalMudanca = 'Data Fornecimento Criada'
				OR PrincipalMudanca = 'Data Fornecimento Deletada'
				
				THEN 0
				ELSE 1
			END
		)
		, MultiplicadorMudancaSubmercado = (
			CASE
				WHEN db.DIFF_Submercado IS NOT NULL THEN 0
				ELSE 1
			END
		)
		, MultiplicadorMudancaFonteEnergia = (
			CASE
				WHEN db.DIFF_FonteEnergia IS NOT NULL THEN 0
				ELSE 1
			END
		)
		, MultiplicadorTipoFlexibilidadePreco = (
			CASE
				WHEN db.DIFF_TipoFlexibilidadePreco IS NOT NULL THEN 0
				ELSE 1
			END
		)

		, DIFF_Portfolio
		, DIFF_Classificacao
		, DIFF_portfolios
		, DIFF_BoletaAtiva
		, DIFF_TipoOperacao
		, DIFF_NaturezaOperacao
		, DIFF_Submercado
		, DIFF_FonteEnergia
		, DIFF_FlexibilidadePreco
		, DIFF_TipoFlexibilidadePreco
		, DIFF_VolumeFinal_MWh
		, DIFF_PrecoContrato
		, DIFF_PrecoFinal
		, DIFF_TetoPreco
		, DIFF_PisoPreco
		, DIFF_Spread
		, DIFF_PrecoPLDRef
		, DIFF_PrecoFuturoRef
		, DIFF_V_Curva_referencia
		, DIFF_V_Curva_Agio
		, DIFF_V_Curva_Spread_Submercado
		, DIFF_V_Curva_spread_fonte_energia
		, DIFF_T_PLD_Preco
		, DIFF_PrecoComparacao_Ref
		, DIFF_PrecoComparacao_Agio
		, DIFF_PrecoComparacao_Spread_Submercado
		, DIFF_PrecoComparacao_Spread_fonte_energia
		, DIFF_PrecoContratoRef
		, DIFF_PrecoComparacao
	from d0_t2 d_0
	left join diferencas_boletas_resumo db
		on d_0.Thunders = db.Thunders
		and d_0.UnidadeNegocio = db.UnidadeNegocio
		and d_0.Codigo = db.Codigo
		and d_0.Entrega = db.Entrega
		and d_0.DataFornecimento = db.DataFornecimento
	where d_0.BoletaAtiva = 1
),
d1_mudancas as (
	select
		d_1.*
		, db.PrincipalMudanca
		, db.PrincipalMudancaCurvaPLD
		, db.PrincipalMudancaPortfolio
		, MultiplicadorResultado = (
			CASE NaturezaOperacao
				WHEN 'Compra' Then -1
				WHEN 'Venda' Then 1
			END
		)
		, MultiplicadorBoletaInativada = (
			CASE
				WHEN DIFF_BoletaAtiva = '0 -> 1' THEN 0
				WHEN DIFF_BoletaAtiva = '1 -> 0' THEN 0
				ELSE 1
			END
		)
		, MultiplicadorRegistroCriadoDeletado = (
			CASE
				WHEN PrincipalMudanca = 'Boleta Criada'
				OR PrincipalMudanca = 'Boleta Deletada'
				OR PrincipalMudanca = 'Entrega Criada'
				OR PrincipalMudanca = 'Entrega Deletada'
				OR PrincipalMudanca = 'Data Fornecimento Criada'
				OR PrincipalMudanca = 'Data Fornecimento Deletada'
				
				THEN 0
				ELSE 1
			END
		)
		, MultiplicadorMudancaSubmercado = (
			CASE
				WHEN db.DIFF_Submercado IS NOT NULL THEN 0
				ELSE 1
			END
		)
		, MultiplicadorMudancaFonteEnergia = (
			CASE
				WHEN db.DIFF_FonteEnergia IS NOT NULL THEN 0
				ELSE 1
			END
		)
		, MultiplicadorTipoFlexibilidadePreco = (
			CASE
				WHEN db.DIFF_TipoFlexibilidadePreco IS NOT NULL THEN 0
				ELSE 1
			END
		)
		, DIFF_Portfolio
		, DIFF_Classificacao
		, DIFF_portfolios
		, DIFF_BoletaAtiva
		, DIFF_TipoOperacao
		, DIFF_NaturezaOperacao
		, DIFF_Submercado
		, DIFF_FonteEnergia
		, DIFF_FlexibilidadePreco
		, DIFF_TipoFlexibilidadePreco
		, DIFF_VolumeFinal_MWh
		, DIFF_PrecoContrato
		, DIFF_PrecoFinal
		, DIFF_TetoPreco
		, DIFF_PisoPreco
		, DIFF_Spread
		, DIFF_PrecoPLDRef
		, DIFF_PrecoFuturoRef
		, DIFF_V_Curva_referencia
		, DIFF_V_Curva_Agio
		, DIFF_V_Curva_Spread_Submercado
		, DIFF_V_Curva_spread_fonte_energia
		, DIFF_T_PLD_Preco
		, DIFF_PrecoComparacao_Ref
		, DIFF_PrecoComparacao_Agio
		, DIFF_PrecoComparacao_Spread_Submercado
		, DIFF_PrecoComparacao_Spread_fonte_energia
		, DIFF_PrecoContratoRef
		, DIFF_PrecoComparacao
	from d1_t2 d_1
	left join diferencas_boletas_resumo db
		on d_1.Thunders = db.Thunders
		and d_1.UnidadeNegocio = db.UnidadeNegocio
		and d_1.Codigo = db.Codigo
		and d_1.Entrega = db.Entrega
		and d_1.DataFornecimento = db.DataFornecimento
	where d_1.BoletaAtiva = 1
),
diferencas_agg as (
	select
		Thunders = ISNULL(d_0.Thunders, d_1.Thunders)
		, UnidadeNegocio = ISNULL(d_0.UnidadeNegocio, d_1.UnidadeNegocio)
		, Codigo = ISNULL(d_0.Codigo, d_1.Codigo)
		, Entrega = ISNULL(d_0.Entrega, d_1.Entrega)
		, DataFornecimento = ISNULL(d_0.DataFornecimento, d_1.DataFornecimento)

		, DataHistorico_d0 = d_0.DataHistorico
		, DataHistorico_d1 = d_1.DataHistorico
		
		, PrincipalMudanca = ISNULL(d_0.PrincipalMudanca, d_1.PrincipalMudanca)
		, PrincipalMudancaCurvaPLD = ISNULL(d_0.PrincipalMudancaCurvaPLD, d_1.PrincipalMudancaCurvaPLD)
		, PrincipalMudancaPortfolio = ISNULL(d_0.PrincipalMudancaPortfolio, d_1.PrincipalMudancaPortfolio)

		, Porfolio = ISNULL(d_0.Portfolio, d_0.Portfolio)
		, Submercado = ISNULL(d_0.Submercado, d_0.Submercado)
		, FonteEnergia = ISNULL(d_0.FonteEnergia, d_0.FonteEnergia)
		

		, VolumeFinal_MWh_d1 = d_1.VolumeFinal_MWh
		, VolumeFinal_MWh_d0 = d_0.VolumeFinal_MWh
		
		, Resultado_d0 = d_0.Resultado
		, Resultado_d1 = d_1.Resultado

		
		, Resultado2_d0 = d_0.Resultado2
		, Resultado2_d1 = d_1.Resultado2



		, Resultado3_d0 = d_0.Resultado3
		, Resultado3_d1 = d_1.Resultado3

		, DIFF_Resultado3 = ISNULL(d_0.Resultado3, 0) - ISNULL(d_1.Resultado3, 0)

		
		, DIFF_Resultado3_Volume =
		ISNULL(d_0.MultiplicadorBoletaInativada, d_1.MultiplicadorBoletaInativada)*
		ISNULL(d_0.MultiplicadorRegistroCriadoDeletado, d_1.MultiplicadorRegistroCriadoDeletado)*
		ISNULL(d_0.MultiplicadorMudancaSubmercado, d_1.MultiplicadorMudancaSubmercado)*
		ISNULL(d_0.MultiplicadorMudancaFonteEnergia, d_1.MultiplicadorMudancaFonteEnergia)*
		ISNULL(d_0.MultiplicadorTipoFlexibilidadePreco, d_1.MultiplicadorTipoFlexibilidadePreco)*
		d_0.MultiplicadorResultado*
		(
			(ISNULL(d_0.VolumeFinal_MWh,0) - ISNULL(d_1.VolumeFinal_MWh,0))
				* (
					d_0.PrecoContratoRef
					- (ISNULL(d_0.PrecoContratoRef,0) - ISNULL(d_1.PrecoContratoRef,0))
					- d_0.PrecoComparacao
					+ (ISNULL(d_0.PrecoComparacao,0) - ISNULL(d_1.PrecoComparacao,0))
			)
		)
		
		, DIFF_Resultado3_PrecoContrato =
		ISNULL(d_0.MultiplicadorBoletaInativada, d_1.MultiplicadorBoletaInativada)*
		ISNULL(d_0.MultiplicadorRegistroCriadoDeletado, d_1.MultiplicadorRegistroCriadoDeletado)*
		ISNULL(d_0.MultiplicadorMudancaSubmercado, d_1.MultiplicadorMudancaSubmercado)*
		ISNULL(d_0.MultiplicadorMudancaFonteEnergia, d_1.MultiplicadorMudancaFonteEnergia)*
		ISNULL(d_0.MultiplicadorTipoFlexibilidadePreco, d_1.MultiplicadorTipoFlexibilidadePreco)*
		d_0.MultiplicadorResultado*((ISNULL(d_0.PrecoContratoRef,0) - ISNULL(d_1.PrecoContratoRef,0))
			* d_0.VolumeFinal_MWh)
		
		, DIFF_Resultado3_CurvaTotal =
		-ISNULL(d_0.MultiplicadorBoletaInativada, d_1.MultiplicadorBoletaInativada)*
		ISNULL(d_0.MultiplicadorRegistroCriadoDeletado, d_1.MultiplicadorRegistroCriadoDeletado)*
		d_0.MultiplicadorResultado*(d_0.VolumeFinal_MWh
			* (ISNULL(d_0.PrecoComparacao,0) - ISNULL(d_1.PrecoComparacao,0)))

		
		, DIFF_Resultado3_CurvaRef =
		-ISNULL(d_0.MultiplicadorBoletaInativada, d_1.MultiplicadorBoletaInativada)*
		ISNULL(d_0.MultiplicadorRegistroCriadoDeletado, d_1.MultiplicadorRegistroCriadoDeletado)*
		ISNULL(d_0.MultiplicadorMudancaSubmercado, d_1.MultiplicadorMudancaSubmercado)*
		ISNULL(d_0.MultiplicadorMudancaFonteEnergia, d_1.MultiplicadorMudancaFonteEnergia)*
		ISNULL(d_0.MultiplicadorTipoFlexibilidadePreco, d_1.MultiplicadorTipoFlexibilidadePreco)*
		d_0.MultiplicadorResultado*(d_0.VolumeFinal_MWh
			* (ISNULL(d_0.PrecoComparacao_Ref,0) - ISNULL(d_1.PrecoComparacao_Ref,0)))
		
		, DIFF_Resultado3_CurvaAgio =  -ISNULL(d_0.MultiplicadorBoletaInativada, d_1.MultiplicadorBoletaInativada)*
		ISNULL(d_0.MultiplicadorRegistroCriadoDeletado, d_1.MultiplicadorRegistroCriadoDeletado)*
		ISNULL(d_0.MultiplicadorMudancaSubmercado, d_1.MultiplicadorMudancaSubmercado)*
		ISNULL(d_0.MultiplicadorMudancaFonteEnergia, d_1.MultiplicadorMudancaFonteEnergia)*
		ISNULL(d_0.MultiplicadorTipoFlexibilidadePreco, d_1.MultiplicadorTipoFlexibilidadePreco)*
		d_0.MultiplicadorResultado*(d_0.VolumeFinal_MWh
			* (ISNULL(d_0.PrecoComparacao_Agio,0) - ISNULL(d_1.PrecoComparacao_Agio,0)))
		
		, DIFF_Resultado3_CurvaSubmercado =  -ISNULL(d_0.MultiplicadorBoletaInativada, d_1.MultiplicadorBoletaInativada)*
		ISNULL(d_0.MultiplicadorRegistroCriadoDeletado, d_1.MultiplicadorRegistroCriadoDeletado)*
		ISNULL(d_0.MultiplicadorMudancaSubmercado, d_1.MultiplicadorMudancaSubmercado)*
		ISNULL(d_0.MultiplicadorMudancaFonteEnergia, d_1.MultiplicadorMudancaFonteEnergia)*
		ISNULL(d_0.MultiplicadorTipoFlexibilidadePreco, d_1.MultiplicadorTipoFlexibilidadePreco)*
		d_0.MultiplicadorResultado*(d_0.VolumeFinal_MWh
			* (ISNULL(d_0.PrecoComparacao_Spread_Submercado,0) - ISNULL(d_1.PrecoComparacao_Spread_Submercado,0)))
		
		, DIFF_Resultado3_CurvaFonteEnergia =  -ISNULL(d_0.MultiplicadorBoletaInativada, d_1.MultiplicadorBoletaInativada)*
		ISNULL(d_0.MultiplicadorRegistroCriadoDeletado, d_1.MultiplicadorRegistroCriadoDeletado)*
		ISNULL(d_0.MultiplicadorMudancaSubmercado, d_1.MultiplicadorMudancaSubmercado)*
		ISNULL(d_0.MultiplicadorMudancaFonteEnergia, d_1.MultiplicadorMudancaFonteEnergia)*
		ISNULL(d_0.MultiplicadorTipoFlexibilidadePreco, d_1.MultiplicadorTipoFlexibilidadePreco)*
		d_0.MultiplicadorResultado*(d_0.VolumeFinal_MWh
			* (ISNULL(d_0.PrecoComparacao_Spread_fonte_energia,0) - ISNULL(d_1.PrecoComparacao_Spread_fonte_energia,0)))

		
		, DIFF_Resultado3_BoletaInativada =
		(CASE WHEN ISNULL(d_0.MultiplicadorBoletaInativada, d_1.MultiplicadorBoletaInativada) = 0 THEN 1 ELSE 0 END) *(
		
		ISNULL(d_0.Resultado3, 0) - ISNULL(d_1.Resultado3, 0))
		
		, DIFF_Resultado3_RegistroCriadoDeletado =
		(CASE WHEN ISNULL(d_0.MultiplicadorRegistroCriadoDeletado, d_1.MultiplicadorRegistroCriadoDeletado) = 0 THEN 1 ELSE 0 END) *(
		
		ISNULL(d_0.Resultado3, 0) - ISNULL(d_1.Resultado3, 0))


		, DIFF_Resultado3_MudancaSubmercado = 
		ISNULL(d_0.MultiplicadorBoletaInativada, d_1.MultiplicadorBoletaInativada)*
		ISNULL(d_0.MultiplicadorRegistroCriadoDeletado, d_1.MultiplicadorRegistroCriadoDeletado)*
		(CASE WHEN ISNULL(d_0.MultiplicadorMudancaSubmercado, d_1.MultiplicadorMudancaSubmercado) = 0 THEN 1 ELSE 0 END) *
		ISNULL(d_0.MultiplicadorMudancaFonteEnergia, d_1.MultiplicadorMudancaFonteEnergia)*
		ISNULL(d_0.MultiplicadorTipoFlexibilidadePreco, d_1.MultiplicadorTipoFlexibilidadePreco)*
		(ISNULL(d_0.Resultado3, 0) - ISNULL(d_1.Resultado3, 0))

		, DIFF_Resultado3_MudancaFonteEnergia = 
		ISNULL(d_0.MultiplicadorBoletaInativada, d_1.MultiplicadorBoletaInativada)*
		ISNULL(d_0.MultiplicadorRegistroCriadoDeletado, d_1.MultiplicadorRegistroCriadoDeletado)*
		ISNULL(d_0.MultiplicadorMudancaSubmercado, d_1.MultiplicadorMudancaSubmercado) *
		(CASE WHEN ISNULL(d_0.MultiplicadorMudancaFonteEnergia, d_1.MultiplicadorMudancaFonteEnergia) = 0 THEN 1 ELSE 0 END) *
		ISNULL(d_0.MultiplicadorTipoFlexibilidadePreco, d_1.MultiplicadorTipoFlexibilidadePreco)*
		(ISNULL(d_0.Resultado3, 0) - ISNULL(d_1.Resultado3, 0))

		, DIFF_Resultado3_MudancaTipoFlexibilidadePreco = 
		ISNULL(d_0.MultiplicadorBoletaInativada, d_1.MultiplicadorBoletaInativada)*
		ISNULL(d_0.MultiplicadorRegistroCriadoDeletado, d_1.MultiplicadorRegistroCriadoDeletado)*
		ISNULL(d_0.MultiplicadorMudancaSubmercado, d_1.MultiplicadorMudancaSubmercado) *
		ISNULL(d_0.MultiplicadorMudancaFonteEnergia, d_1.MultiplicadorMudancaFonteEnergia)*
		(CASE WHEN ISNULL(d_0.MultiplicadorTipoFlexibilidadePreco, d_1.MultiplicadorTipoFlexibilidadePreco) = 0 THEN 1 ELSE 0 END) *
		(ISNULL(d_0.Resultado3, 0) - ISNULL(d_1.Resultado3, 0))


		, DIFF_Portfolio = ISNULL(d_0.DIFF_Portfolio,d_1.DIFF_Portfolio)
		, DIFF_Classificacao = ISNULL(d_0.DIFF_Classificacao,d_1.DIFF_Classificacao)
		, DIFF_portfolios = ISNULL(d_0.DIFF_portfolios,d_1.DIFF_portfolios)
		, DIFF_BoletaAtiva = ISNULL(d_0.DIFF_BoletaAtiva,d_1.DIFF_BoletaAtiva)
		, DIFF_TipoOperacao = ISNULL(d_0.DIFF_TipoOperacao,d_1.DIFF_TipoOperacao)
		, DIFF_NaturezaOperacao = ISNULL(d_0.DIFF_NaturezaOperacao,d_1.DIFF_NaturezaOperacao)
		, DIFF_Submercado = ISNULL(d_0.DIFF_Submercado,d_1.DIFF_Submercado)
		, DIFF_FonteEnergia = ISNULL(d_0.DIFF_FonteEnergia,d_1.DIFF_FonteEnergia)
		, DIFF_FlexibilidadePreco = ISNULL(d_0.DIFF_FlexibilidadePreco,d_1.DIFF_FlexibilidadePreco)
		, DIFF_TipoFlexibilidadePreco = ISNULL(d_0.DIFF_TipoFlexibilidadePreco,d_1.DIFF_TipoFlexibilidadePreco)
		, DIFF_VolumeFinal_MWh = ISNULL(d_0.DIFF_VolumeFinal_MWh,d_1.DIFF_VolumeFinal_MWh)
		, DIFF_PrecoContrato = ISNULL(d_0.DIFF_PrecoContrato,d_1.DIFF_PrecoContrato)
		, DIFF_PrecoFinal = ISNULL(d_0.DIFF_PrecoFinal,d_1.DIFF_PrecoFinal)
		, DIFF_TetoPreco = ISNULL(d_0.DIFF_TetoPreco,d_1.DIFF_TetoPreco)
		, DIFF_PisoPreco = ISNULL(d_0.DIFF_PisoPreco,d_1.DIFF_PisoPreco)
		, DIFF_Spread = ISNULL(d_0.DIFF_Spread,d_1.DIFF_Spread)
		, DIFF_PrecoPLDRef = ISNULL(d_0.DIFF_PrecoPLDRef,d_1.DIFF_PrecoPLDRef)
		, DIFF_PrecoFuturoRef = ISNULL(d_0.DIFF_PrecoFuturoRef,d_1.DIFF_PrecoFuturoRef)
		, DIFF_V_Curva_referencia = ISNULL(d_0.DIFF_V_Curva_referencia,d_1.DIFF_V_Curva_referencia)
		, DIFF_V_Curva_Agio = ISNULL(d_0.DIFF_V_Curva_Agio,d_1.DIFF_V_Curva_Agio)
		, DIFF_V_Curva_Spread_Submercado = ISNULL(d_0.DIFF_V_Curva_Spread_Submercado,d_1.DIFF_V_Curva_Spread_Submercado)
		, DIFF_V_Curva_spread_fonte_energia = ISNULL(d_0.DIFF_V_Curva_spread_fonte_energia,d_1.DIFF_V_Curva_spread_fonte_energia)
		, DIFF_T_PLD_Preco = ISNULL(d_0.DIFF_T_PLD_Preco,d_1.DIFF_T_PLD_Preco)
		, DIFF_PrecoComparacao_Ref = ISNULL(d_0.DIFF_PrecoComparacao_Ref,d_1.DIFF_PrecoComparacao_Ref)
		, DIFF_PrecoComparacao_Agio = ISNULL(d_0.DIFF_PrecoComparacao_Agio,d_1.DIFF_PrecoComparacao_Agio)
		, DIFF_PrecoComparacao_Spread_Submercado = ISNULL(d_0.DIFF_PrecoComparacao_Spread_Submercado,d_1.DIFF_PrecoComparacao_Spread_Submercado)
		, DIFF_PrecoComparacao_Spread_fonte_energia = ISNULL(d_0.DIFF_PrecoComparacao_Spread_fonte_energia,d_1.DIFF_PrecoComparacao_Spread_fonte_energia)
		, DIFF_PrecoContratoRef = ISNULL(d_0.DIFF_PrecoContratoRef,d_1.DIFF_PrecoContratoRef)
		, DIFF_PrecoComparacao = ISNULL(d_0.DIFF_PrecoComparacao,d_1.DIFF_PrecoComparacao)

	from d0_mudancas d_0
	full outer join d1_mudancas d_1
	on
		d_0.Thunders = d_1.Thunders
		AND d_0.UnidadeNegocio = d_1.UnidadeNegocio
		AND d_0.Codigo = d_1.Codigo
		AND d_0.Entrega = d_1.Entrega
		AND d_0.DataFornecimento = d_1.DataFornecimento
),
final as (
	select
		DataHistorico_d0 = CAST(@data_historico_d0 AS DATE)
		, DataHistorico_d1 = CAST(@data_historico_d1 AS DATE)
		, d.Thunders
		, d.UnidadeNegocio
		, d.Codigo
		, d.Entrega
		, d.DataFornecimento
		
		, d.PrincipalMudanca
		, d.PrincipalMudancaCurvaPLD
		, d.PrincipalMudancaPortfolio 

		, d.Porfolio
		, d.Submercado
		, d.FonteEnergia
		

		, d.VolumeFinal_MWh_d1
		, d.VolumeFinal_MWh_d0
		
		, d.Resultado_d0
		, d.Resultado_d1

		
		, d.Resultado2_d0
		, d.Resultado2_d1

		, d.Resultado3_d0
		, d.Resultado3_d1

		, d.DIFF_Resultado3
		, d.DIFF_Resultado3_Volume
		, d.DIFF_Resultado3_PrecoContrato
		, DIFF_Resultado3_CurvaTotal
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
			WHEN d.DataFornecimento < CONVERT(DATE, (SELECT top 1 DataHistorico from Modelo.dbo.proc_POC_Historico_portfolio_d0)) THEN 'REALIZADO'
			WHEN d.DataFornecimento <= DATEADD(dd,360, CONVERT(DATE, (SELECT top 1 DataHistorico from Modelo.dbo.proc_POC_Historico_portfolio_d0))) THEN 'CIRCULANTE'
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

		, DIFF_Portfolio
		, DIFF_Classificacao
		, DIFF_portfolios
		, DIFF_BoletaAtiva
		, DIFF_TipoOperacao
		, DIFF_NaturezaOperacao
		, DIFF_Submercado
		, DIFF_FonteEnergia
		, DIFF_FlexibilidadePreco
		, DIFF_TipoFlexibilidadePreco
		, DIFF_VolumeFinal_MWh
		, DIFF_PrecoContrato
		, DIFF_PrecoFinal
		, DIFF_TetoPreco
		, DIFF_PisoPreco
		, DIFF_Spread
		, DIFF_PrecoPLDRef
		, DIFF_PrecoFuturoRef
		, DIFF_V_Curva_referencia
		, DIFF_V_Curva_Agio
		, DIFF_V_Curva_Spread_Submercado
		, DIFF_V_Curva_spread_fonte_energia
		, DIFF_T_PLD_Preco
		, DIFF_PrecoComparacao_Ref
		, DIFF_PrecoComparacao_Agio
		, DIFF_PrecoComparacao_Spread_Submercado
		, DIFF_PrecoComparacao_Spread_fonte_energia
		, DIFF_PrecoContratoRef
		, DIFF_PrecoComparacao

	


	from diferencas_agg d
	left join Modelo.dbo.proc_POC_Historico_vpl_d0 v0
		on d.DataFornecimento = v0.datafornecimento
	left join Modelo.dbo.proc_POC_Historico_vpl_d1 v1
		on d.DataFornecimento = v1.datafornecimento


)
SELECT *  FROM final;