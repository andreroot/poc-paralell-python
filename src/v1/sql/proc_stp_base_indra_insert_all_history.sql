BEGIN

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

DECLARE @DataHistorico varchar(100);
DECLARE @DataLogHistorico DATETIME;

DECLARE @n_datas AS INT = 1;
DECLARE @it AS INT = 0;

	BEGIN
	WHILE @it < @n_datas
		BEGIN
			SET @DataHistorico = (select concat(CONVERT(DATE, DATEADD(DAY, -@it, max(data)), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial');	
			SET @DataLogHistorico = SYSDATETIME();

			EXEC bookindra.Book.STP_CriarOperationHistorico @dataHistorico;

			IF OBJECT_ID('tempdb..#TempTableAllHistory') IS NOT NULL
			BEGIN
				DROP TABLE #TempTableAllHistory;
			END

				-- Note que foi colocado um 'Físico as TipoContrato' porque a procedure não está funcionando isso...
				SELECT 
					CONVERT(DATE, @DataHistorico, 103) AS DataHistorico
					, *
					, @DataLogHistorico AS ProcessInsertTimeInic
				INTO #TempTableAllHistory
				FROM (
					select * from  bookindra.Thunders.[VW_OperationHistorico]
				)
				TempTableAllHistory;

				--SELECT * FROM #TempTableAllHistory;

				DELETE FROM [modelo].[BaseHistorica].[BoletasProcessadasv2] 
				WHERE DataHistorico = CONVERT(DATE, @DataHistorico, 103) 
				AND Thunders = 'Indra';

				INSERT INTO [modelo].[BaseHistorica].[BoletasProcessadasv2]
				SELECT * FROM #TempTableAllHistory;

				COMMIT;

			SET @it = @it + 1;
		END;
	END;

END;