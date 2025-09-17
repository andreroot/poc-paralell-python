/**bases geradas pelo processo STP_gera_bases_POC_Historico**/
--base portfolio - base function Modelo.dbo.[s_all_operations_portfolio]  -critico
SELECT max(DataCriacao), max(DateTimeCriacao) FROM Modelo.dbo.proc_POC_Historico_portfolio_d0;
SELECT max(DataCriacao), max(DateTimeCriacao) FROM Modelo.dbo.proc_POC_Historico_portfolio_d1;

-- Bases Curva - base book.curva.[VW_Curva_Fwd3] 
SELECT max(DATA) FROM Modelo.dbo.proc_POC_Historico_Curva_d0;
SELECT max(DATA) FROM Modelo.dbo.proc_POC_Historico_Curva_d1;

-- base Curva Diff - base gerada proc_POC_Historico_Curva_d0 join proc_POC_Historico_Curva_d1
SELECT max("Data Curva D+0") FROM Modelo.dbo.proc_POC_Historico_DIFF_Curva;

-- Bases de PLD - base Book.[Curva].[PLD_Oficial]
SELECT  max(DataInsert), max(data), count(1)  FROM Modelo.dbo.proc_POC_Historico_PLD_d0;
SELECT max(data), count(1)  FROM Modelo.dbo.proc_POC_Historico_PLD_d1;

-- Bases PLD Diff - proc_POC_Historico_PLD_d0 join proc_POC_Historico_PLD_d1
SELECT max(data), count(1) FROM Modelo.dbo.proc_POC_Historico_DIFF_PLD;

-- Bases de VPL - base proc_POC_Historico_portfolio_d0 - critico
SELECT max(data_vpl) FROM Modelo.dbo.proc_POC_Historico_vpl_d0;
SELECT max(data_vpl) FROM Modelo.dbo.proc_POC_Historico_vpl_d1;

/**[dbo].[STP_gera_BI_POC_Historico]**/

select max(DataHistorico) from Modelo.dbo.proc_POC_Historico_resultado_d0

select max(DataHistorico_d0), max(DataHistorico_d1) from Modelo.[POC_Historico].[Diferencas_agg]

select max(DataHistorico_d0), max(DataHistorico_d1) from Modelo.dbo.proc_POC_Historico_diff_BI