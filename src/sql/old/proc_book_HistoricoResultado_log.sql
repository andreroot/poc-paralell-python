BEGIN

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

DECLARE @DataInicio as varchar(100);
SET @DataInicio = (select max(data) start_date from book.curva.Curva_Fwd where curva = 'Oficial');
--select DATEADD(DAY, -1, max(data)) start_date from book.curva.Curva_Fwd where curva = 'Oficial'

DECLARE @DataFim as varchar(100);
SET @DataFim = (select max(data) start_date from book.curva.Curva_Fwd where curva = 'Oficial');

DECLARE @Curva     VARCHAR(250);
SET @Curva = 'Oficial'

EXEC BOOK.[STP_HistoricoResultado_log] @DataInicio, @DataFim, @Curva;;


-- LOG
IF OBJECT_ID('tempdb..#TempTableLogHistory') IS NOT NULL
BEGIN
	DROP TABLE #TempTableLogHistory;
END
-- Tabela recebe dados historicos
SELECT * INTO #TempTableLogHistory
FROM (
	SELECT *
	FROM BOOK.[HistoricoResultado_log]
    WHERE DataHistorico  >= CONVERT( DATE, @DataInicio)
) TempTableLogHistory;

COMMIT;

SELECT * FROM #TempTableLogHistory;

END






