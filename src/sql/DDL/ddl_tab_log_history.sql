USE [modelo]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE TABLE [modelo].[BaseHistorica].[LogHistOperationPortifolio](  
LogID                                   INT IDENTITY(1,1) PRIMARY KEY,
Thunders								varchar(250) NULL, 
ProcessoExecutado						varchar(250) NULL, 
Etapa			                        varchar(250) NULL,
DataHistorico                           varchar(250) NULL,                     
QtdeRows                                varchar(250) NULL,
ProcessInsertTimeInic                   varchar(250) NULL,
ProcessInsertTimeFim                    varchar(250) NULL,
StatusProcesso							varchar(250) NULL,
MsgError								NVARCHAR(MAX) NULL
) ON [PRIMARY]
GO

CREATE TABLE [modelo].[BaseHistorica].[LogDataDiffHistPosicaoLog](  
Thunders								varchar(250) NULL,  
DataHistorico                           varchar(250) NULL,                  
MinDataFornecimento                     varchar(250) NULL,
TotalVolumeFinal_MWh                    decimal(20, 8) NULL, 
TotalVolumeFinal_MWm                    decimal(20, 8) NULL, 
TotalPrecoContrato                      decimal(20, 8) NULL, 
TotalPrecoFinal                         decimal(20, 8) NULL, 
QtdeRows                                varchar(250) NULL,
ProcessInsertTime                       varchar(250) NULL
) ON [PRIMARY]
GO
			SELECT Thunders, DataHistorico, min(datafornecimento) MinDataFornecimento, sum(precoContrato) TotalprecoContrato, count(1) QtdeRows,  @DataLogHistorico ProcessInsertTime
































































































































