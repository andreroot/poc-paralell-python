
BEGIN

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

DECLARE @DataHistorico as varchar(100);
SET @DataHistorico = (SELECT concat(CONVERT(DATE, DATEADD(DAY, -7, GETDATE()), 103),'T23:59:59.000Z'));

DECLARE @DataDelecao as varchar(100);
SET @DataDelecao = (SELECT concat(CONVERT(DATE, DATEADD(DAY, -7, GETDATE()), 103),'T00:00:00.000Z'));
--SET @DataHistorico = (select concat(CONVERT(DATE, DATEADD(DAY, -1, (max(data))), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial');
-- (SELECT concat(CONVERT(DATE, DATEADD(DAY, -4, GETDATE()), 103),'T23:59:59.000Z'))

DECLARE @DataLogHistorico as varchar(100);
SET @DataLogHistorico = (SELECT CONVERT(VARCHAR(100), SYSDATETIME(), 121));

DECLARE @ErrorMessage NVARCHAR(MAX) = NULL;
DECLARE @LogID INT;

-- Inserir o log de início
INSERT INTO [modelo].[BaseHistorica].[LogBoletasProcessadas] (Thunders, ProcessoExecutado, Etapa, DataHistorico, QtdeRows, ProcessInsertTimeInic, StatusProcesso  )
VALUES ('all', '[modelo].[BaseHistorica].[BoletasProcessadas]','delete_rows_historico',@DataHistorico, 0,  @DataLogHistorico, 'Iniciado');

-- Capturar o ID do log para atualização posterior
SET @LogID = SCOPE_IDENTITY();

    IF OBJECT_ID('tempdb..#TempDeleteHistory') IS NOT NULL
    BEGIN
        DROP TABLE #TempDeleteHistory;
    END

        SELECT * INTO #TempDeleteHistory
        FROM (
        SELECT COUNT(1) QtdeRows FROM [modelo].[BaseHistorica].[BoletasProcessadas] WHERE (ProcessInsertTimeInic < @DataDelecao OR ProcessInsertTimeInic IS NULL) AND DataHistorico < @DataHistorico
        ) TempDeleteHistory;


        ---delete na tabela

        DELETE FROM [modelo].[BaseHistorica].[BoletasProcessadas]
        WHERE (ProcessInsertTimeInic < @DataDelecao OR ProcessInsertTimeInic IS NULL)
        AND DataHistorico < @DataHistorico;

    -- Generate divide-by-zero error.
		SET @DataLogHistorico = (SELECT CONVERT(VARCHAR(100), SYSDATETIME(), 121));


		BEGIN TRY


				-- Inserir o log de início
				-- INSERT INTO [modelo].[BaseHistorica].[LogBoletasProcessadas] (Thunders, ProcessoExecutado, Etapa, DataHistorico, QtdeRows, ProcessInsertTimeInic, StatusProcesso  )
				-- VALUES ('Comercial', 'BookComercial.dbo.s_all_operations(@DataHistorico)','extrair_dados_comerial',@DataHistorico, 0, 'Iniciado')

				-- Atualizar o log com o tempo de término e status de sucesso
				UPDATE [modelo].[BaseHistorica].[LogBoletasProcessadas]
				SET ProcessInsertTimeFim = @DataLogHistorico, QtdeRows= (select QtdeRows from #TempDeleteHistory), StatusProcesso = 'Sucesso'
				WHERE LogID = @LogID;
		END TRY

		BEGIN CATCH
				-- Capturar qualquer erro ocorrido
				SET @ErrorMessage = ERROR_MESSAGE()

				-- Atualizar o log com o tempo de término e status de sucesso
				UPDATE [modelo].[BaseHistorica].[LogBoletasProcessadas]
				SET ProcessInsertTimeFim = @DataLogHistorico, QtdeRows= 0, StatusProcesso = 'Erro', MsgError = @ErrorMessage
				WHERE LogID = @LogID;
		END CATCH;

		COMMIT;

END

SELECT * FROM #TempDeleteHistory;
