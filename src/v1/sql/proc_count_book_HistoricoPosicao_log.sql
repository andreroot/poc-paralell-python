
BEGIN

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

    DECLARE @DataInicio as varchar(100);
    SET @DataInicio = (SELECT concat(CONVERT(DATE, DATEADD(DAY, 0, max(data)), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial');
                    
    -- LOG
    IF OBJECT_ID('tempdb..#TempTableLogHistory') IS NOT NULL
    BEGIN
        DROP TABLE #TempTableLogHistory;
    END
    -- Tabela recebe dados historicos
    SELECT * INTO #TempTableLogHistory
    FROM (
        SELECT *
        FROM book.HistoricoPosicao_log  
        WHERE DataHistorico = @DataInicio
        -- where convert(date, DataHistorico, 103) >=  '2025-03-01'
        --     and FonteEnergia = 'Não Previsto'
    ) TempTableLogHistory;

    SELECT * FROM #TempTableLogHistory;

END;