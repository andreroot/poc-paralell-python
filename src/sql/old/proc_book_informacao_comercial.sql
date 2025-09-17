BEGIN

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

DECLARE @DataInicio as varchar(100);
SET @DataInicio = (select max(data) start_date from book.curva.Curva_Fwd where curva = 'Oficial');
--select DATEADD(DAY, -1, max(data)) start_date from book.curva.Curva_Fwd where curva = 'Oficial'

DECLARE @Curva     VARCHAR(250);
SET @Curva = 'Oficial'

exec [Book].[InformacaoComercial] @DataInicio, @Curva;

-- LOG
IF OBJECT_ID('tempdb..#TempInformacaoComercial_table6') IS NOT NULL
BEGIN
	DROP TABLE #TempInformacaoComercial_table6;
END
-- Tabela recebe dados historicos
SELECT * INTO #TempInformacaoComercial_table6
FROM (
	SELECT *
	FROM BOOK.[BOOK].proc_InformacaoComercial_table6
    ) TempInformacaoComercial_table6;


COMMIT;

SELECT * FROM #TempInformacaoComercial_table6;

END