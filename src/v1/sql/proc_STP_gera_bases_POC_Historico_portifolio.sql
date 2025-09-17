

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;
	
BEGIN

	DECLARE @vpl AS NVARCHAR(255) = (select top 1 vpl from treinamento.gpires.tabelavpl where dateInsert = (SELECT MAX(dateInsert) FROM treinamento.gpires.tabelavpl));
	DECLARE @n_datas AS INT = 2;
	DECLARE @it AS INT = 1;
	DECLARE @data_historico_d0 AS DATE;
	DECLARE @data_historico_d1 AS DATE;
	DECLARE @datas AS TABLE ([Data] DATE, Ordem INT);


	INSERT INTO @datas
	SELECT
		[Data],
		RN
	FROM (
		SELECT
			[Data],
			rn = ROW_NUMBER() OVER (ORDER BY [Data] desc)
		FROM (
			SELECT DISTINCT
				[Data]
			FROM Book.Curva.Curva_FWD
			WHERE Curva = 'Oficial'
			AND [Data] >= (SELECT MIN(DataHistorico_d1) FROM Modelo.[POC_Historico].[Diferencas_agg])
		) x
	) y
	WHERE RN <= @n_datas;


		BEGIN
		WHILE @it < @n_datas
			BEGIN
				SET @data_historico_d0 = (SELECT [Data] FROM @datas WHERE ORDEM = @it)
				SET @data_historico_d1 = (SELECT [Data] FROM @datas WHERE ORDEM = @it + 1)
				SET @it = @it + 1;
		
				EXEC Modelo.dbo.STP_gera_bases_POC_Historico 
					@vpl=@vpl,
					@data_d0=@data_historico_d0,
					@data_d1=@data_historico_d1,
					@curva1='Oficial',
					@curva2='Oficial',
					@anofornecimento_min=2024;

			END;
					
		END;

END;