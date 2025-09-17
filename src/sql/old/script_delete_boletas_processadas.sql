--SELECT * FROM  [modelo].[BaseHistorica].[BoletasProcessadas]
--WHERE  CONVERT(DATE, DataHistorico, 103) = '2024-12-02';

BEGIN

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

DECLARE @DataHistorico as varchar(100);
-- SET @DataHistorico = (select concat(max(data),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial');
DECLARE @n_datas AS INT = 10;
DECLARE @it AS INT = 1;
DECLARE @data_historico_d0 AS DATE;


	BEGIN
	WHILE @it < @n_datas
		BEGIN
			--PRINT(GETDATE())
			SET @data_historico_d0 = (select concat(CONVERT(DATE, DATEADD(DAY, @it-60, (max(data))), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial')
			

			DELETE FROM [modelo].[BaseHistorica].[BoletasProcessadasv2]
            WHERE  CONVERT(DATE, DataHistorico, 103) = @data_historico_d0;


			PRINT(@it)

			PRINT(@data_historico_d0)
			SET @it = @it + 1;

		END

	END
END