
BEGIN


    SET NOCOUNT ON;
    SET ANSI_WARNINGS OFF;

    DECLARE @DataInicio as varchar(100);
    --SET @DataInicio = (SELECT CONCAT(CONVERT(DATE, DATEADD(DAY, 0, max(data)), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial');
    SET @DataInicio = (SELECT  CONCAT(CONVERT(DATE,DATEADD(DAY, -30, max(data)), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial');

    DECLARE @DataFim as varchar(100);
    --SET @DataInicio = (SELECT CONCAT(CONVERT(DATE, DATEADD(DAY, 0, max(data)), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial');
    SET @DataFim = (SELECT  CONCAT(CONVERT(DATE,DATEADD(DAY, -1, max(data)), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial');
                                     
    -- LOG
    IF OBJECT_ID('tempdb..#TempTableLogHistory') IS NOT NULL
    BEGIN
        DROP TABLE #TempTableLogHistory;
    END
    -- Tabela recebe dados historicos
    SELECT * INTO #TempTableLogHistory
    FROM (
        SELECT *
        FROM Book.Book.proc_InformacaoComercial_table6
--        where convert(date, version, 103) between @DataInicio and  @DataFim 
		WHERE DataFornecimento >=  '2025-01-01' --and  '2025-03-30'
          AND year = 2025
    ) TempTableLogHistory;

    SELECT * FROM #TempTableLogHistory;

END;