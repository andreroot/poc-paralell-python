BEGIN

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

	DECLARE @DataLogHistorico as varchar(100);
	DECLARE @DataHistorico as varchar(100);

	SET @DataHistorico = (SELECT CONCAT(max(data),'T23:59:59.000Z') start_date FROM book.curva.Curva_Fwd WHERE curva = 'Oficial');
	SET @DataLogHistorico = (SELECT CONVERT(VARCHAR(100), SYSDATETIME(), 121));

	-- LOG
	IF OBJECT_ID('tempdb..#TempTableLogHistory') IS NOT NULL
	BEGIN
		DROP TABLE #TempTableLogHistory;
	END

	-- Tabela log
	SELECT * INTO #TempTableLogHistory
	FROM (
		SELECT Thunders, DataHistorico, count(1) QtdeRows,  @DataLogHistorico ProcessInsertTime
			FROM [modelo].[BaseHistorica].[BoletasProcessadas]
			WHERE DataHistorico >= (select concat(CONVERT(DATE, DATEADD(DAY, -3, (max(data))), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial')
			GROUP BY Thunders, DataHistorico
	) TempTableLogHistory;


	INSERT INTO [modelo].[BaseHistorica].[LogBoletasProcessadas]
	SELECT * FROM #TempTableLogHistory;
END

/** VALIDACAO DO LOG
SELECT A.* FROM (
	SELECT * 
	, ROW_NUMBER() over(partition by Thunders, DataHistorico order by ProcessInsertTime desc ) rown_idx
	FROM  [modelo].[BaseHistorica].[LogBoletasProcessadas]
	WHERE DataHistorico >= (select concat(CONVERT(DATE, DATEADD(DAY, -3, (max(data))), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial')
--	ORDER BY ProcessInsertTime DESC
) A WHERE A.rown_idx = 1
	ORDER BY Thunders, DataHistorico DESC;
	**/
