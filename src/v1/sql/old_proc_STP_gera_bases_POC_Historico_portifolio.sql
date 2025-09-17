

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;
	
BEGIN

	DECLARE @vpl AS NVARCHAR(255) = (select top 1 vpl from treinamento.gpires.tabelavpl where dateInsert = (SELECT MAX(dateInsert) FROM treinamento.gpires.tabelavpl));
	DECLARE @n_datas AS INT = 2;
	DECLARE @it AS INT = 1;
	DECLARE @data_historico_d0 AS DATE;
	DECLARE @data_historico_d1 AS DATE;
	DECLARE @datas AS TABLE ([Data] DATE, Ordem INT);


	INSERT INTO @datas
	SELECT
		[Data],
		RN
	FROM (
		SELECT
			[Data],
			rn = ROW_NUMBER() OVER (ORDER BY [Data] desc)
		FROM (
			SELECT DISTINCT
				[Data]
			FROM Book.Curva.Curva_FWD
			WHERE Curva = 'Oficial'
			AND [Data] >= (SELECT MIN(DataHistorico_d1) FROM Modelo.[POC_Historico].[Diferencas_agg])
		) x
	) y
	WHERE RN <= @n_datas;


		BEGIN
		WHILE @it < @n_datas
			BEGIN
				PRINT(GETDATE())
				SET @data_historico_d0 = (SELECT [Data] FROM @datas WHERE ORDEM = @it)
				SET @data_historico_d1 = (SELECT [Data] FROM @datas WHERE ORDEM = @it + 1)
				SET @it = @it + 1;
		
				EXEC Modelo.dbo.STP_gera_bases_POC_Historico 
					@vpl=@vpl,
					@data_d0=@data_historico_d0,
					@data_d1=@data_historico_d1,
					@curva1='Oficial',
					@curva2='Oficial',
					@anofornecimento_min=2024;


				PRINT(@data_historico_d0)
				PRINT(@data_historico_d1)

				--EXEC Modelo.dbo.STP_gera_BI_POC_Historico;

				DELETE FROM Modelo.[POC_Historico].[Diferencas_agg]
				WHERE DataHistorico_d0 = @data_historico_d0
				AND DataHistorico_d1 = @data_historico_d1;

			BEGIN
				IF OBJECT_ID('tempdb..#TempTablestoploss') IS NOT NULL
				BEGIN
					DROP TABLE #TempTablestoploss;
				END

				SELECT * INTO #TempTablestoploss
				FROM (
					SELECT 
						DataHistorico_d0,
						DataHistorico_d1,
						DataFornecimento,
						PrincipalMudanca,
						Portfolio,
						VolumeNet_MWh_d0 = SUM(VolumeNet_MWh_d0),
						VolumeNet_MWh_d1 = SUM(VolumeNet_MWh_d1),
						DIFF_VolumeNet_MWh = SUM(DIFF_VolumeNet_MWh),
						[Resultado d+0] = SUM([Resultado d+0]),
						[Resultado d-1] = SUM([Resultado d-1]),
						DIFF_Resultado3 = SUM(DIFF_Resultado3),
						DIFF_Resultado3_BoletaInativada = SUM(DIFF_Resultado3_BoletaInativada),
						DIFF_Resultado3_CurvaAgio = SUM(DIFF_Resultado3_CurvaAgio),
						DIFF_Resultado3_CurvaFonteEnergia = SUM(DIFF_Resultado3_CurvaFonteEnergia),
						DIFF_Resultado3_CurvaRef = SUM(DIFF_Resultado3_CurvaRef),
						DIFF_Resultado3_CurvaSubmercado = SUM(DIFF_Resultado3_CurvaSubmercado),
						DIFF_Resultado3_MudancaFonteEnergia = SUM(DIFF_Resultado3_MudancaFonteEnergia),
						DIFF_Resultado3_MudancaSubmercado = SUM(DIFF_Resultado3_MudancaSubmercado),
						DIFF_Resultado3_MudancaTipoFlexibilidadePreco = SUM(DIFF_Resultado3_MudancaTipoFlexibilidadePreco),
						DIFF_Resultado3_PrecoContrato = SUM(DIFF_Resultado3_PrecoContrato),
						DIFF_Resultado3_RegistroCriadoDeletado = SUM(DIFF_Resultado3_RegistroCriadoDeletado),
						DIFF_Resultado3_Volume = SUM(DIFF_Resultado3_Volume)
					FROM Modelo.dbo.proc_POC_Historico_diff_BI
					GROUP BY
						DataHistorico_d0,
						DataHistorico_d1,
						DataFornecimento,
						PrincipalMudanca,
						Portfolio
				) TempTablestoploss;

				--> insert into na tabela usando [tabela alinhada] stoploss
				INSERT INTO Modelo.[POC_Historico].[Diferencas_agg](
					DataHistorico_d0,
					DataHistorico_d1,
					DataFornecimento,
					PrincipalMudanca,
					Portfolio,
					VolumeNet_MWh_d0,
					VolumeNet_MWh_d1,
					DIFF_VolumeNet_MWh,
					[Resultado d+0],
					[Resultado d-1],
					DIFF_Resultado3,
					DIFF_Resultado3_BoletaInativada,
					DIFF_Resultado3_CurvaAgio,
					DIFF_Resultado3_CurvaFonteEnergia,
					DIFF_Resultado3_CurvaRef,
					DIFF_Resultado3_CurvaSubmercado,
					DIFF_Resultado3_MudancaFonteEnergia,
					DIFF_Resultado3_MudancaSubmercado,
					DIFF_Resultado3_MudancaTipoFlexibilidadePreco,
					DIFF_Resultado3_PrecoContrato,
					DIFF_Resultado3_RegistroCriadoDeletado,
					DIFF_Resultado3_Volume)
				SELECT DataHistorico_d0,
					DataHistorico_d1,
					DataFornecimento,
					PrincipalMudanca,
					Portfolio,
					VolumeNet_MWh_d0,
					VolumeNet_MWh_d1,
					DIFF_VolumeNet_MWh,
					[Resultado d+0],
					[Resultado d-1],
					DIFF_Resultado3,
					DIFF_Resultado3_BoletaInativada,
					DIFF_Resultado3_CurvaAgio,
					DIFF_Resultado3_CurvaFonteEnergia,
					DIFF_Resultado3_CurvaRef,
					DIFF_Resultado3_CurvaSubmercado,
					DIFF_Resultado3_MudancaFonteEnergia,
					DIFF_Resultado3_MudancaSubmercado,
					DIFF_Resultado3_MudancaTipoFlexibilidadePreco,
					DIFF_Resultado3_PrecoContrato,
					DIFF_Resultado3_RegistroCriadoDeletado,
					DIFF_Resultado3_Volume
				FROM #TempTablestoploss;

				COMMIT;
			END;

		END;
					
	END;

END;