DECLARE @vpl AS NVARCHAR(255) = (select top 1 vpl from treinamento.gpires.tabelavpl where dateInsert = (SELECT MAX(dateInsert) FROM treinamento.gpires.tabelavpl));
DECLARE @data_curva_d0 AS DATE = (select max(data) from book.curva.Curva_Fwd where curva = 'Oficial');
DECLARE @data_curva_d1 AS DATE = (select max(data) from book.curva.Curva_Fwd where curva = 'Oficial' and data < @data_curva_d0);

EXEC Modelo.dbo.STP_gera_bases_POC_Historico @vpl, @data_curva_d0, @data_curva_d1, 'Oficial', 'Oficial', NULL;

EXEC Modelo.dbo.STP_gera_BI_POC_Historico;

DELETE FROM Modelo.[POC_Historico].[Diferencas_agg]
WHERE DataHistorico_d0 = @data_curva_d0
AND DataHistorico_d1 = @data_curva_d1;

WITH stoploss as (
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
)
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
    DIFF_Resultado3_Volume
)
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
FROM stoploss;