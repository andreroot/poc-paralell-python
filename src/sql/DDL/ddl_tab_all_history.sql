USE [modelo]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE SCHEMA BaseHistorica
go
--> NOVA IMPLEMETAÇÃO NA TABELA ALTERAR NOME E INCLUIR DATA DE PROCESSTIME (TIMESTAMP INSERÇÃO DO DADO NA TABELA)
--> USAR ESSA VARIAVEL PARA INSERCAO
-->DECLARE @DataLogHistorico as varchar(100);
-->SET @DataLogHistorico = (SELECT CONVERT(VARCHAR(100), SYSDATETIME(), 121));

CREATE TABLE [modelo].[BaseHistorica].[BoletasProcessadasv2](  
DataHistorico                              DATE    NULL,  
Codigo                                     varchar(250)    NULL,                          
Entrega                                    varchar(250)    NULL,                         
TipoOperacao                               varchar(250)    NULL,                            
NaturezaOperacao                           varchar(250)    NULL,                            
version                                    varchar(250)    NULL,                         
primaryOperationId                         varchar(250)    NULL,                          
primaryOperationCode                       varchar(250)    NULL,                            
isTrading                                  bit             NULL,                  
isServices                                 bit             NULL,                 
isGeneration                               bit             NULL,                   
UnidadeNegocio                             varchar(250)    NULL,                          
BoletaAtiva                                bit             NULL,                
IDParte                                    varchar(250)    NULL,                         
CNPJParte                                  varchar(250)    NULL,                           
EmpresaResponsavel                         varchar(250)    NULL,                          
SiglaParte                                 varchar(250)    NULL,                          
SiglaAgenteParte                           varchar(250)    NULL,                            
CodAgenteParte                             varchar(250)    NULL,                          
CodPerfilParte                             varchar(250)    NULL,                          
SiglaPerfilParte                           varchar(250)    NULL,                            
IDContraparte                              varchar(250)    NULL,                           
CNPJContraparte                            varchar(250)    NULL,                         
Negociante                                 varchar(250)    NULL,                          
SiglaContraparte                           varchar(250)    NULL,                            
SiglaAgenteContraparte                     varchar(250)    NULL,                          
CodAgenteContraparte                       varchar(250)    NULL,                            
CodPerfilContraparte                       varchar(250)    NULL,                            
SiglaPerfilContraparte                     varchar(250)    NULL,                          
ContrapartePertenceGrupo                   bit             NULL,                   
Operador                                   varchar(250)    NULL,                            
Submercado                                 varchar(250)    NULL,                          
FonteEnergia                               varchar(250)    NULL,                            
FlexibilidadePreco                         varchar(250)    NULL,                          
InicioFornecimento                         varchar(250)    NULL,                          
FimFornecimento                            varchar(250)    NULL,                         
VolumeContratado_MWh                       decimal(20, 8)  NULL,                              
VolumeContratado_MWm                       decimal(20, 8)  NULL,                              
VolumeSazonalizado_MWh                     decimal(20, 8)  NULL,                            
VolumeSazonalizado_MWm                     decimal(20, 8)  NULL,                            
VolumePrevisto_MWh                         decimal(20, 8)  NULL,                            
VolumePrevisto_MWm                         decimal(20, 8)  NULL,                            
VolumeMinimoPrevistoFlex_MWh               decimal(20, 8)  NULL,                              
VolumeMinimoPrevistoFlex_MWm               decimal(20, 8)  NULL,                              
VolumeMaximoPrevistoFlex_MWh               decimal(20, 8)  NULL,                              
VolumeMaximoPrevistoFlex_MWm               decimal(20, 8)  NULL,                              
VolumeFinal_MWh                            decimal(20, 8)  NULL,                           
VolumeFinal_MWm                            decimal(20, 8)  NULL,                           
VolumeMinimoPrevistoFlexNet_MWh            decimal(20, 8)  NULL,                           
VolumeMinimoPrevistoFlexNet_MWm            decimal(20, 8)  NULL,                           
VolumeMaximoPrevistoFlexNet_MWh            decimal(20, 8)  NULL,                           
VolumeMaximoPrevistoFlexNet_MWm            decimal(20, 8)  NULL,                           
VolumeNet_Mwh                              decimal(20, 8)  NULL,                             
VolumeNet_Mwm                              decimal(20, 8)  NULL,                             
VolumeMWmDeltaFinalPrevisto                decimal(20, 8)  NULL,                           
VolumeMWhDeltaFinalPrevisto                decimal(20, 8)  NULL,                           
EstadoFlexibilidade_MWm                    varchar(250)    NULL,                         
FlexibilidadeExercida_MWm                  decimal(20, 8)  NULL,                             
Renegociacao_MWm                           decimal(20, 8)  NULL,                              
PrecoContrato                              decimal(20, 8)  NULL,                             
PrecoFinal                                 decimal(20, 8)  NULL,                            
PrecoAjustado                              decimal(20, 8)  NULL,                             
NotaFiscal                                 decimal(20, 8)  NULL,                            
MTM                                        decimal(20, 8)  NULL,                           
RETUSD                                     varchar(250)    NULL,                          
Classificacao                              varchar(250)    NULL,                           
UserCriador                                varchar(250)    NULL,                         
DataCriacao                                varchar(250)    NULL,                         
DateTimeCriacao                            varchar(250)    NULL,                         
UserModificador                            varchar(250)    NULL,                         
DataModificacao                            varchar(250)    NULL,                         
DateTimeModificacao                        varchar(250)    NULL,                         
UserDelete                                 varchar(250)    NULL,                          
DataDelete                                 varchar(250)    NULL,                          
DateTimeDelete                             varchar(250)    NULL,                          
UserBackoffice                             varchar(250)    NULL,                          
UserComercial                              varchar(250)    NULL,                           
OrigemOperacao                             varchar(250)    NULL,                          
ContratoBBCE                               varchar(250)    NULL,                            
FlexibilidadeMensal                        bit             NULL,                
isFlexibilityLoadCurve                     bit             NULL,                 
isFlexibilityByPeriod                      bit             NULL,                  
PorcentagemFlexibilidadeInferior           float           NULL,                     
PorcentagemFlexibilidadeSuperior           float           NULL,                     
Sazonalizacao                              bit             NULL,                  
isSeasonalityByPeriod                      bit             NULL,                  
PorcentagemSazonalizacaoInferior           float           NULL,                     
PorcentagemSazonalizacaoSuperior           float           NULL,                     
PossuiModulacao                            bit             NULL,                
isModulationLoadCurve                      bit             NULL,                  
hasDefaultFinancialFlow                    bit             NULL,                
PossuiReajuste                             bit             NULL,                 
IndiceReajuste                             varchar(250)    NULL,                          
DataBase1                                  varchar(250)    NULL,                           
Data1Reajuste                              varchar(250)    NULL,                           
PossuiGarantia                             bit             NULL,                 
ValorGarantia                              decimal(20, 8)  NULL,                             
DataApresentacaoGarantia                   varchar(250)    NULL,                            
TipoGarantia                               varchar(250)    NULL,                            
hasRepresentativeFactor                    bit             NULL,                
representativeFactorPercent                decimal(20, 8)  NULL,                           
losses                                     decimal(20, 8)  NULL,                            
ContratoCCEE                               varchar(250)    NULL,                            
needApportionment                          varchar(250)    NULL,                           
Spread                                     decimal(20, 8)  NULL,                            
PisoPreco                                  decimal(20, 8)  NULL,                             
TetoPreco                                  decimal(20, 8)  NULL,                             
StatusCobranca                             varchar(250)    NULL,                          
IDStatusAprovacao                          varchar(250)    NULL,                           
aprovalStatusDescription                   varchar(250)    NULL,                            
AnoFornecimento                            varchar(250)    NULL,                         
MesFornecimento                            varchar(250)    NULL,                         
DiaInicioFornecimento                      varchar(250)    NULL,                           
DiaFimFornecimento                         varchar(250)    NULL,                          
ID                                         varchar(250)    NULL,                          
operationTypeId                            varchar(250)    NULL,                         
_Link                                      varchar(250)    NULL,                           
Negocio                                    varchar(250)    NULL,                         
TipoNegocio                                varchar(250)    NULL,                         
Portfolio                                  varchar(250)    NULL,                           
DataFornecimento                           varchar(250)    NULL,                            
MesCriacao                                 varchar(250)    NULL,                          
DuracaoContrato                            varchar(250)    NULL,                         
Thunders                                   varchar(250)    NULL,                            
portfolios                                 varchar(250)    NULL,                          
PrecoContratoComReajuste                   decimal(20, 8)  NULL,                              
ProcessInsertTimeInic                      DATETIME    NULL,    
) ON [PRIMARY]
GO

       
CREATE INDEX indx_unique_boletas_historicos_1 ON [modelo].[BaseHistorica].[BoletasProcessadasv2] ([Thunders], [DataHistorico])
GO                


-- CREATE INDEX indx_unique_boletas_historicos_1 ON [modelo].[BaseHistorica].[BoletasProcessadas] ([Thunders], [DataFornecimento])
-- GO

-- CREATE INDEX indx_unique_boletas_historicos_2 ON [modelo].[BaseHistorica].[BoletasProcessadas] ([Codigo], [Entrega], [AnoFornecimento], [MesFornecimento], [NaturezaOperacao])
-- GO

-- CREATE INDEX indx_unique_boletas_historicos_3 ON [modelo].[BaseHistorica].[BoletasProcessadas] ([Thunders], [DataFornecimento], [TipoOperacao])
-- GO

-- CREATE INDEX indx_unique_boletas_historicos_4 ON [modelo].[BaseHistorica].[BoletasProcessadas] ([DataHistorico])
-- GO

-- CREATE INDEX indx_unique_boletas_historicos_5 ON [modelo].[BaseHistorica].[BoletasProcessadas] ([ProcessInsertTimeInic])
-- GO

-- CREATE INDEX indx_unique_boletas_historicos_9 ON [modelo].[BaseHistorica].[BoletasProcessadas] ([DataHistorico])
-- GO

--> ALTER TABLE [modelo].[BaseHistorica].[BoletasProcessadas]
--> ADD ProcessInsertTimeInic                   varchar(250) NULL

-- DataHistorico	datetime
-- DataFornecimento	date
-- isTrading	bit
-- isServices	bit
-- isGeneration	bit
-- Thunders	nvarchar(20)
-- EmpresaResponsavel	nvarchar(255)
-- TipoNegocio	nvarchar(20)
-- Classificacao	nvarchar(50)
-- TipoContrato	nvarchar(20)
-- TipoOperacao	nvarchar(20)
-- NaturezaOperacao	nvarchar(20)
-- Submercado	nvarchar(20)
-- FonteEnergia	nvarchar(50)
-- FlexibilidadePreco	nvarchar(50)
-- TipoFlexibilidadePreco	nvarchar(50)
-- VolumeFinal_MWh	float
-- VolumeFinal_MWm	float
-- PrecoContrato	float
-- PrecoFinal	float
-- Spread	float
-- TetoPreco	float
-- PisoPreco	float
	