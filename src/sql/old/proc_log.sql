BEGIN

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

DECLARE @DataLogHistorico as varchar(100);
SET @DataLogHistorico = (SELECT CONVERT(VARCHAR(100), SYSDATETIME(), 121));

DECLARE @ErrorMessage NVARCHAR(MAX) = NULL;
DECLARE @LogID INT;

-- Inserir o log de início
INSERT INTO [modelo].[BaseHistorica].[LogBoletasProcessadas] (Thunders, ProcessoExecutado, Etapa, DataHistorico, QtdeRows, ProcessInsertTimeInic, StatusProcesso  )
VALUES ('Comercial', 'BookComercial.dbo.s_all_operations(@DataHistorico)','extrair_dados_comercial',@DataHistorico, 0,  @DataLogHistorico, 'Iniciado');

-- Capturar o ID do log para atualização posterior
SET @LogID = SCOPE_IDENTITY();


-- Generate divide-by-zero error.
SET @DataLogHistorico = (SELECT CONVERT(VARCHAR(100), SYSDATETIME(), 121));

-- LOG
IF OBJECT_ID('tempdb..#TempTableLogHistory') IS NOT NULL
BEGIN
	DROP TABLE #TempTableLogHistory;
END
-- Tabela recebe dados historicos
SELECT * INTO #TempTableLogHistory
FROM (
	SELECT count(1) QtdeRows
		FROM [modelo].[BaseHistorica].[BoletasProcessadas]
	WHERE DataHistorico = @DataHistorico
		AND Thunders = 'Comercial'
) TempTableLogHistory;

BEGIN TRY


		-- Inserir o log de início
		-- INSERT INTO [modelo].[BaseHistorica].[LogBoletasProcessadas] (Thunders, ProcessoExecutado, Etapa, DataHistorico, QtdeRows, ProcessInsertTimeInic, StatusProcesso  )
		-- VALUES ('Comercial', 'BookComercial.dbo.s_all_operations(@DataHistorico)','extrair_dados_comerial',@DataHistorico, 0, 'Iniciado')

		-- Atualizar o log com o tempo de término e status de sucesso
		UPDATE [modelo].[BaseHistorica].[LogBoletasProcessadas]
		SET ProcessInsertTimeFim = @DataLogHistorico, QtdeRows= (select QtdeRows from #TempTableLogHistory), StatusProcesso = 'Sucesso'
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



DECLARE @DataLogHistorico as varchar(100);
SET @DataLogHistorico = (SELECT CONVERT(VARCHAR(100), SYSDATETIME(), 121));

DECLARE @ErrorMessage NVARCHAR(MAX) = NULL;
DECLARE @LogID INT;

-- Inserir o log de início
INSERT INTO [modelo].[BaseHistorica].[LogBoletasProcessadas] (Thunders, ProcessoExecutado, Etapa, DataHistorico, QtdeRows, ProcessInsertTimeInic, StatusProcesso  )
VALUES ('Comercial', 'BookComercial.dbo.s_all_operations(@DataHistorico)','extrair_dados_comercial',@DataHistorico, 0,  @DataLogHistorico, 'Iniciado');

-- Capturar o ID do log para atualização posterior
SET @LogID = SCOPE_IDENTITY();



-- Generate divide-by-zero error.
SET @DataLogHistorico = (SELECT CONVERT(VARCHAR(100), SYSDATETIME(), 121));


BEGIN TRY
        -- Atualizar o log com o tempo de término e status de sucesso
        UPDATE [modelo].[BaseHistorica].[LogBoletasProcessadas]
        SET ProcessInsertTimeFim = @DataLogHistorico, QtdeRows= (select count(1) QtdeRows from #TempTableLogHistory), StatusProcesso = 'Sucesso'
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