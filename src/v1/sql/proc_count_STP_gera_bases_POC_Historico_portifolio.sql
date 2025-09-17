SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;
    BEGIN
        IF OBJECT_ID('tempdb..#TempBoletas') IS NOT NULL
        BEGIN
            DROP TABLE #TempBoletas;
        END

        SELECT * INTO #TempBoletas
        FROM (
                SELECT * 
                FROM Modelo.dbo.proc_POC_Historico_portfolio_d0
                UNION ALL
                SELECT * 
                FROM Modelo.dbo.proc_POC_Historico_portfolio_d1
                ) TempBoletas;
        

        SELECT * FROM #TempBoletas;

    END;