
SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;
BEGIN

	DECLARE @DataHistorico varchar(100);
	SET @DataHistorico = (select concat(max(data),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial');

	IF OBJECT_ID('tempdb..#TempBoletas') IS NOT NULL
	BEGIN
		DROP TABLE #TempBoletas;
	END

	SELECT * INTO #TempBoletas
	FROM (
			SELECT *,'Físico' as TipoContrato 
			FROM [modelo].[BaseHistorica].[BoletasProcessadasv2]
			WHERE BoletaAtiva = 1
			-- Remove as boletas apagadas
			AND UnidadeNegocio != 'Serviços'
			-- Seleciona Só as boletas mais recentes
			AND year(DataFornecimento) >= Year(GETDATE()) - 1
			AND Thunders = 'Safira'
			AND DataHistorico = CONVERT(DATE, @DataHistorico, 103)
			--AND DataHistorico = DATEADD(DAY, 1, CONVERT(DATE, @DataHistorico, 103))
			) TempBoletas;
	--WHERE a.DataCriacao = '';

	SELECT * FROM #TempBoletas;


END;