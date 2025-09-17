DECLARE @vpl AS NVARCHAR(255) = (select top 1 vpl from treinamento.gpires.tabelavpl where dateInsert = (SELECT MAX(dateInsert) FROM treinamento.gpires.tabelavpl));
DECLARE @n_datas AS INT = 30;
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
		rn = ROW_NUMBER() OVER (ORDER BY [Data] DESC)
	FROM (
		SELECT DISTINCT
			[Data]
		FROM Book.Curva.Curva_FWD
		WHERE Curva = 'Oficial'
		--ORDER BY [Data] DESC
		AND [Data] <= (SELECT MIN(DataHistorico_d1) FROM Modelo.[POC_Historico].[Diferencas_agg])
	) x
) y
WHERE RN <= @n_datas;

--SELECT * FROM @datas;

BEGIN
WHILE @it < @n_datas
	BEGIN
		PRINT(GETDATE())
		SET @data_historico_d0 = (SELECT [Data] FROM @datas WHERE ORDEM = @it)
		SET @data_historico_d1 = (SELECT [Data] FROM @datas WHERE ORDEM = @it + 1)
		SET @it = @it + 1;

		PRINT(@data_historico_d0)
		PRINT(@data_historico_d1)

	END
END
