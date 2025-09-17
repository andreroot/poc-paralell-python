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

			EXEC Book.Book.STP_CriarOperationHistorico @dataHistorico;

			EXEC BookComercial.Book.STP_CriarOperationHistorico @dataHistorico;

			EXEC BookIndra.Book.STP_CriarOperationHistorico @dataHistorico;

			--SELECT @DataHistorico, @DataLogHistorico 

			IF OBJECT_ID('tempdb..#TempTableAllHistory') IS NOT NULL
			BEGIN
				DROP TABLE #TempTableAllHistory;
			END

				-- Note que foi colocado um 'Físico as TipoContrato' porque a procedure não está funcionando isso...
				SELECT 
					CONVERT(DATE, @DataHistorico, 103) AS DataHistorico
					, *
					, @DataLogHistorico AS ProcessInsertTimeInic
				INTO #TempTable
				FROM (
					select * from  book.Thunders.[VW_OperationHistorico]
					UNION ALL
					select * from  bookcomercial.Thunders.[VW_OperationHistorico]
					UNION ALL
					select # from  bookindra.Thunders.[VW_OperationHistorico]
					-- select *,'Físico' as TipoContrato from  bookindra.Thunders.[VW_OperationHistorico]
					-- 	Where 
					-- 	-- Remove as boletas apagadas
					-- 	BoletaAtiva = 1
					-- 	AND UnidadeNegocio != 'Serviços'
					-- 	-- Seleciona Só as boletas mais recentes
					-- 	AND year(DataFornecimento) >= Year(GETDATE()) - 1
				)
				TempTableAllHistory;

				--SELECT * FROM #TempTableAllHistory;

				DELETE FROM [modelo].[BaseHistorica].[BoletasProcessadasv2] 
				WHERE DataHistorico = CONVERT(DATE, @DataHistorico, 103) 
				AND Thunders = 'Comercial';

				INSERT INTO [modelo].[BaseHistorica].[BoletasProcessadasv2]
				SELECT * FROM #TempTableAllHistory;

				COMMIT;

			SET @it = @it + 1;
		END;
	END;

END;