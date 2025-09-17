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
			SELECT Thunders
				, DataHistorico
				, min(datafornecimento) MinDataFornecimento, 
				, sum(VolumeFinal_MWh) TotalVolumeFinal_MWh
				, sum(VolumeFinal_MWm) TotalVolumeFinal_MWm
				, sum(PrecoContrato) TotalPrecoContrato
				, sum(PrecoFinal) TotalPrecoFinal
				, count(1) QtdeRows
				,  @DataLogHistorico ProcessInsertTime
			  FROM  (
			SELECT 
				ROW_NUMBER() over(partition by   datafornecimento,thunders,classificacao,tiponegocio, tipocontrato, tipooperacao, naturezaoperacao, submercado, fonteenergia, flexibilidadepreco order by DataHistorico asc) rown_idx
				, datafornecimento
				, thunders
				, classificacao
				, tiponegocio
				, tipocontrato
				, tipooperacao
				, naturezaoperacao
				, submercado
				, fonteenergia
				, flexibilidadepreco
				, precoContrato
				, DataHistorico
				--, DENSE_RANK() over(partition by   datafornecimento,thunders,classificacao,tiponegocio, tipocontrato, tipooperacao, naturezaoperacao, submercado, fonteenergia, flexibilidadepreco order by precoContrato asc) des_rak_idx
				, RANK() over(partition by   datafornecimento,thunders,classificacao,tiponegocio, tipocontrato, tipooperacao, naturezaoperacao, submercado, fonteenergia, flexibilidadepreco order by precoContrato asc) rak_idx
				FROM book.HistoricoPosicao_log
				WHERE DataHistorico >=  (select concat(CONVERT(DATE, DATEADD(DAY, -4, (max(data))), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial')
				) h
			WHERE   h.datafornecimento < (select CONVERT(DATE, DATEADD(MONTH, -6, (GETDATE())), 103) )--'2024-09-01'

		) TempTableLogHistory;


		INSERT INTO [modelo].[BaseHistorica].[LogDataDiffHistPosicaoLog]
		SELECT Thunders
			, DataHistorico
			, MinDataFornecimento
			, TotalVolumeFinal_MWh
			, TotalVolumeFinal_MWm
			, TotalPrecoContrato
			, TotalPrecoFinal
			, QtdeRows
			, ProcessInsertTime
		FROM #TempTableLogHistory where rak_idx > 1;



END
