


BEGIN

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

DECLARE @DataHistorico as varchar(100);
SET @DataHistorico = (select concat(max(data),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial');
--SET @DataHistorico = (select concat(CONVERT(DATE, DATEADD(DAY, -1, (max(data))), 103),'T23:59:59.000Z') start_date from book.curva.Curva_Fwd where curva = 'Oficial')

DECLARE @DataLogHistorico as varchar(100);
SET @DataLogHistorico = (SELECT CONVERT(VARCHAR(100), SYSDATETIME(), 121));

DECLARE @ErrorMessage NVARCHAR(MAX) = NULL;
DECLARE @LogID INT;

-- Capturar o ID do log para atualização posterior
SET @LogID = SCOPE_IDENTITY();

IF OBJECT_ID('tempdb..#TempTableAllHistory') IS NOT NULL
	BEGIN
		DROP TABLE #TempTableAllHistory;
	END
	-- Tabela recebe dados historicos
	SELECT 
		@DataHistorico AS DataHistorico
		,* 
		 INTO #TempTableAllHistory
	FROM (
	SELECT 
						A.[code]																																																							AS Codigo
						,A.[sequence]																																																						AS Entrega
						,A.[operationType]																																																					AS TipoOperacao
						,A.[tradeType]																																																						AS NaturezaOperacao
						,A.[version]																																																						AS [version]																	
						,A.[primaryOperationId]																																																				AS primaryOperationId																				
						,A.[primaryOperationCode]																																																			AS primaryOperationCode
						,A.[isTrading]																																																						AS isTrading
						,A.[isServices]																																																						AS isServices			
						,A.[isGeneration]																																																					AS isGeneration	
						,A.[businessUnitDescription]																																																		AS UnidadeNegocio
						,A.[isActive]																																																						AS BoletaAtiva					
						,A.[partyId]																																																						AS IDParte				
						,A.[partyCNPJ]																																																						AS CNPJParte				
						,A.[partyName]																																																						AS EmpresaResponsavel					
						,A.[partyAlias]																																																						AS SiglaParte					
						,A.[partyAgentAcronym]																																																				AS SiglaAgenteParte				
						,A.[partyAgentCode]																																																					AS CodAgenteParte				
						,A.[partyProfileCode]																																																				AS CodPerfilParte			
						,A.[partyProfileDescription]																																																		AS SiglaPerfilParte	
						,A.[counterpartyId]																																																					AS IDContraparte			
						,A.[counterpartyCNPJ]																																																				AS CNPJContraparte			
						,A.[counterpartyName]																																																				AS Negociante				
						,A.[counterpartyAlias]																																																				AS SiglaContraparte					
						,A.[counterpartyAgentAcronym]																																																		AS SiglaAgenteContraparte
						,CASE 
							WHEN A.[counterpartyAgentCode] = NULL AND A.[counterpartyAgentAcronym] <> 'MIGRAÇÃO' THEN NULL
							ELSE A.[counterpartyAgentCode]																																																	
						END																																																									AS CodAgenteContraparte
						,CASE 
							WHEN A.[counterpartyProfileCode] = NULL AND A.[counterpartyAgentAcronym] <> 'MIGRAÇÃO' THEN NULL
							ELSE A.[counterpartyProfileCode]																																																	
						END																																																									AS CodPerfilContraparte																																																																																
						,A.[counterpartyProfileDescription]																																																	AS SiglaPerfilContraparte
						,A.[counterPartyIsGroupCompany]																																																		AS ContrapartePertenceGrupo	
						,A.[userOperatorName]																																																				AS Operador			
						,CASE WHEN A.[submarketDescription] = 'Sudeste' THEN 'SE'
							WHEN A.[submarketDescription] = 'SUL' THEN 'S'
							WHEN A.[submarketDescription] = 'Nordeste' THEN 'NE'
							WHEN A.[submarketDescription] = 'Norte' THEN 'N'
							ELSE 'Não Previsto'
						END																																																									AS Submercado	
						,CASE WHEN A.[energySourceDescription] = 'Convencional' THEN 'Convencional'
							WHEN A.[energySourceDescription] = 'Incentivada 100%' THEN '100% Incent.'
							WHEN A.[energySourceDescription] = 'Incentivada 50%' THEN '50% Incent.'
							WHEN A.[energySourceDescription] = 'Incentivada 0%' THEN '0% Incent.'
							WHEN A.[energySourceDescription] = 'Cogeração Qualificada 50%' Then 'CQ5'
							WHEN A.[energySourceDescription] = 'Cogeração Qualificada 100%' Then 'CQ1'
							ELSE 'Não Previsto'
						END																																																									AS FonteEnergia	
						,A.[priceTypeDescription]																																																			AS FlexibilidadePreco		
						,CONVERT(DATE,A.[startDate])																																																		AS InicioFornecimento					
						,CONVERT(DATE,A.[endDate])																																																			AS FimFornecimento
						,A.[contractedVolumeMwh]																																																			AS VolumeContratado_MWh
						,A.[contractedVolumeMwm]																																																			AS VolumeContratado_MWm
						,A.[seasonalityVolumeMwh]																																																			AS VolumeSazonalizado_MWh
						,A.[seasonalityVolumeMwm]																																																			AS VolumeSazonalizado_MWm	
						,ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh])																																											AS VolumePrevisto_MWh					
						,ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])																																											AS VolumePrevisto_MWm	
						,CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh])	
							ELSE ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh]) * (100-A.[flexibilityPercentageBottom])/100
						END			 																																																					AS VolumeMinimoPrevistoFlex_MWh					
						,CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
							ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100-A.[flexibilityPercentageBottom])/100
						END			 																																																					AS VolumeMinimoPrevistoFlex_MWm					
						,CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh])	
							ELSE ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh]) * (100+A.[flexibilityPercentageBottom])/100
						END			 																																																					AS VolumeMaximoPrevistoFlex_MWh					
						,CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
							ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100+A.[flexibilityPercentageBottom])/100
						END																																																								AS VolumeMaximoPrevistoFlex_MWm	
						,A.[finalVolumeMwh]																																																					AS VolumeFinal_MWh		
						,A.[finalVolumeMwm]																																																					AS VolumeFinal_MWm
						,(CASE WHEN A.[tradeType]='COMPRA' THEN 1 ELSE -1 END) * CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh])	
																						ELSE ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh]) * (100-A.[flexibilityPercentageBottom])/100
																					END			 																																						AS VolumeMinimoPrevistoFlexNet_MWh					
						,(CASE WHEN A.[tradeType]='COMPRA' THEN 1 ELSE -1 END) * CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
																						ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100-A.[flexibilityPercentageBottom])/100
																					END			 																																						AS VolumeMinimoPrevistoFlexNet_MWm					
						,(CASE WHEN A.[tradeType]='COMPRA' THEN 1 ELSE -1 END) * CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh])	
																						ELSE ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh]) * (100+A.[flexibilityPercentageBottom])/100
																					END			 																																						AS VolumeMaximoPrevistoFlexNet_MWh					
						,(CASE WHEN A.[tradeType]='COMPRA' THEN 1 ELSE -1 END) * CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
																						ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100+A.[flexibilityPercentageBottom])/100
																					END																																									AS VolumeMaximoPrevistoFlexNet_MWm		
						,(CASE WHEN A.[tradeType]='COMPRA' THEN 1 ELSE -1 END) * A.[finalVolumeMwh]																																						AS VolumeNet_Mwh
						,(CASE WHEN A.[tradeType]='COMPRA' THEN 1 ELSE -1 END) * A.[finalVolumeMwm]																																						AS VolumeNet_Mwm		
						
						
						
						,(A.[finalVolumeMwm] - ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]))																																															AS VolumeMWmDeltaFinalPrevisto
						,(A.[finalVolumeMwh] - ISNULL(A.[seasonalityVolumeMwh],A.[contractedVolumeMwh]))																																															AS VolumeMWhDeltaFinalPrevisto
						,CASE WHEN (A.[finalVolumeMwm] = ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])) THEN 'Igual'
							WHEN (A.[finalVolumeMwm] < ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])) THEN CASE WHEN (A.[finalVolumeMwm] < CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
																																							ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100-A.[flexibilityPercentageBottom])/100
																																						END) THEN 'Recompra'
																															ELSE 'Limite Take Minimo'
																														END 
							WHEN (A.[finalVolumeMwm] > ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])) THEN CASE WHEN (A.[finalVolumeMwm] > CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
																																							ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100+A.[flexibilityPercentageBottom])/100
																																						END) THEN 'Revenda'
																															ELSE 'Limite Take Maximo'
																														END 
							ELSE 'Nao Tratado'
						END																																																																			AS EstadoFlexibilidade_MWm		
						,CASE	WHEN (A.[finalVolumeMwm] = ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])) THEN (A.[finalVolumeMwm] - ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]))
								WHEN (A.[finalVolumeMwm] < ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])) THEN CASE WHEN (A.[finalVolumeMwm] < CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
																																							ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100-A.[flexibilityPercentageBottom])/100
																																						END) THEN (CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
																																										ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100-A.[flexibilityPercentageBottom])/100
																																									END - ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]))
																															ELSE (A.[finalVolumeMwm] - ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]))
																														END 
								WHEN (A.[finalVolumeMwm] > ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])) THEN CASE WHEN (A.[finalVolumeMwm] > CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
																																							ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100+A.[flexibilityPercentageBottom])/100
																																						END) THEN (CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
																																										ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100+A.[flexibilityPercentageBottom])/100
																																									END - ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]))
																															ELSE (A.[finalVolumeMwm] - ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]))
																														END 
								ELSE NULL
						END																																																																			AS FlexibilidadeExercida_MWm	
						,ROUND(CASE WHEN (A.[finalVolumeMwm] = ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])) THEN NULL
									WHEN (A.[finalVolumeMwm] < ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])) THEN CASE WHEN (A.[finalVolumeMwm] < CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
																																								ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100-A.[flexibilityPercentageBottom])/100
																																							END) THEN ((A.[finalVolumeMwm] - CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
																																																	ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100-A.[flexibilityPercentageBottom])/100
																																															END))
																																ELSE NULL
																															END 
									WHEN (A.[finalVolumeMwm] > ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])) THEN CASE WHEN (A.[finalVolumeMwm] > CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
																																ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100+A.[flexibilityPercentageBottom])/100
																															END) THEN ((A.[finalVolumeMwm] - CASE WHEN A.[hasFlexibility]  = 0 THEN ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm])	
																																								ELSE ISNULL(A.[seasonalityVolumeMwm],A.[contractedVolumeMwm]) * (100+A.[flexibilityPercentageBottom])/100
																																							END))
																																ELSE NULL
																															END 
									ELSE NULL
								END,6)																																																																AS Renegociacao_MWm	
						
						
						
						
						,A.[basePrice]																																																						AS PrecoContrato			
						,A.[price]																																																							AS PrecoFinal
						,CASE 
							WHEN A.basePrice = A.price THEN NULL
							ELSE A.price
						END																																																									AS PrecoAjustado							
						,A.[nominalPrice]																																																					AS NotaFiscal				
						,A.[mtm]																																																							AS MTM						
						,A.[retusd]																																																							AS RETUSD						
						,A.[classifications]																																																				AS Classificacao			
						,CASE A.[userCreatedName]
							WHEN 'ANDRESSA FERREIRA GOUVEIA' THEN 'Andressa Ferreira Gouveia'
							WHEN 'Larissa Nunes Santos'	THEN 'Larissa Nunes'
							WHEN 'Márcio Davanzo' THEN 'Marcio Queiroz Davanzo'
							WHEN 'Mikio Kawai' THEN 'Mikio Kawai Jr'
							ELSE A.[userCreatedName]
						END																																																									AS UserCriador				
						,CONVERT(DATE,A.[createdAt])																																																		AS DataCriacao
						,A.[createdAt]																																																						AS DateTimeCriacao																																										
						,A.[userModifiedName]																																																				AS UserModificador				
						,CONVERT(DATE,A.[modifiedAt])																																																		AS DataModificacao
						,A.[modifiedAt]																																																						AS DateTimeModificacao					
						,A.[userDeletedName]																																																				AS UserDelete				
						,CONVERT(DATE,A.[deletedAt])																																																		AS DataDelete
						,A.[deletedAt]																																																						AS DateTimeDelete
						,A.[userBackofficeName]																																																				AS UserBackoffice			
						,A.[userCommercialName]																																																				AS UserComercial			
						,A.[origin]																																																							AS OrigemOperacao					
						,A.[bbceCode]																																																						AS ContratoBBCE						
						,A.[hasFlexibility]																																																					AS FlexibilidadeMensal				
						,A.[isFlexibilityLoadCurve]																																																			AS isFlexibilityLoadCurve
						,A.[isFlexibilityByPeriod]																																																			AS isFlexibilityByPeriod
						,A.[flexibilityPercentageBottom]																																																	AS PorcentagemFlexibilidadeInferior	
						,A.[flexibilityPercentageTop]																																																		AS PorcentagemFlexibilidadeSuperior	
						,A.[hasSeasonality]																																																					AS Sazonalizacao			
						,A.[isSeasonalityByPeriod] 																																																			AS isSeasonalityByPeriod
						,A.[seasonalityPercentageBottom]																																																	AS PorcentagemSazonalizacaoInferior 	
						,A.[seasonalityPercentageTop]																																																		AS PorcentagemSazonalizacaoSuperior	
						,A.[hasModulation]																																																					AS PossuiModulacao					
						,A.[isModulationLoadCurve]																																																			AS isModulationLoadCurve
						,A.[hasDefaultFinancialFlow]																																																		AS hasDefaultFinancialFlow
						,A.[hasReadjustment]																																																				AS PossuiReajuste			
						,CASE 
							WHEN A.[reajustmentIndex] IS NULL THEN 'Sem Reajuste'
							ELSE  A.[reajustmentIndex]
						END																																																									AS IndiceReajuste				
						,convert(date,A.[readjustmentBaseDate])																																																AS DataBase1		
						,convert(date,A.[readjustmentFirstDate])																																															AS Data1Reajuste			
						,A.[hasGuarantee]																																																					AS PossuiGarantia					
						,A.[guaranteeValue]																																																					AS ValorGarantia				
						,CONVERT(DATE,A.[guaranteeDueDate])																																																	AS DataApresentacaoGarantia			
						,A.[guaranteeTypes]																																																					AS TipoGarantia			
						,A.[hasRepresentativeFactor]																																																		AS hasRepresentativeFactor
						,A.[representativeFactorPercent]																																																	AS representativeFactorPercent
						,A.[losses]																																																							AS losses	
						,A.[cceeContractCode]																																																				AS ContratoCCEE				
						,A.[needApportionment]																																																				AS needApportionment
						,A.[spread]																																																							AS Spread						
						,A.[floor]																																																							AS PisoPreco					
						,A.[ceiling]																																																						AS TetoPreco					
						,A.[billingStatus]																																																					AS StatusCobranca					
						,A.[aprovalStatusId]																																																				AS IDStatusAprovacao				
						,A.[aprovalStatusDescription]																																																		AS aprovalStatusDescription
						,A.[year]																																																							AS AnoFornecimento						
						,A.[month]																																																							AS MesFornecimento							
						,A.[startDay]																																																						AS DiaInicioFornecimento				
						,A.[endDay]																																																							AS DiaFimFornecimento					
						,A.[id]																																																								AS ID							
						,A.[operationTypeId]																																																				AS operationTypeId	
						,A.[_Link]																																																							AS _Link	
						,CASE
								WHEN SUBSTRING(A.Code, 1, 2) = 'IC' AND A.[partyName] = 'SAFIRA ADMINISTRACAO E COMERCIALIZACAO DE ENERGIA S.A.' and A.[counterpartyName] = 'ARTEMIS COMERCIALIZACAO DE ENERGIA LTDA.' then 'Negocio'
								WHEN SUBSTRING(A.Code, 1, 2) = 'IC' AND A.[partyName] = 'SAFIRA ADMINISTRACAO E COMERCIALIZACAO DE ENERGIA S.A.' and A.[counterpartyName] = 'SAFIRA GESTAO E CONSULTORIA EM ENERGIA LTDA' then 'Negocio'
								WHEN SUBSTRING(A.Code, 1, 2) = 'IC' AND A.[partyName] = 'SAFIRA ADMINISTRACAO E COMERCIALIZACAO DE ENERGIA S.A.' and A.[counterpartyName] = 'SAFIRA TRADING E GERACAO DE ENERGIA LTDA.' then 'Negocio'
								WHEN SUBSTRING(A.Code, 1, 2) = 'IC' AND A.[partyName] = 'SAFIRA GESTAO E CONSULTORIA EM ENERGIA LTDA' and A.[counterpartyName] = 'ARTEMIS COMERCIALIZACAO DE ENERGIA LTDA.' then 'Negocio'
								WHEN SUBSTRING(A.Code, 1, 2) = 'IC' AND A.[partyName] = 'SAFIRA GESTAO E CONSULTORIA EM ENERGIA LTDA' and A.[counterpartyName] = 'SAFIRA TRADING E GERACAO DE ENERGIA LTDA.' then 'Negocio'
								WHEN SUBSTRING(A.Code, 1, 2) = 'IC' AND A.[partyName] = 'SAFIRA GESTAO E CONSULTORIA EM ENERGIA LTDA' and A.[counterpartyName] = 'SAFIRA ADMINISTRACAO E COMERCIALIZACAO DE ENERGIA S.A.' then 'Negocio'
								WHEN SUBSTRING(A.Code, 1, 2) = 'IC' AND A.[partyName] = 'ARTEMIS COMERCIALIZACAO DE ENERGIA LTDA.' and A.[counterpartyName] = 'SAFIRA ADMINISTRACAO E COMERCIALIZACAO DE ENERGIA S.A.' then 'Negocio'
								WHEN SUBSTRING(A.Code, 1, 2) = 'IC' AND A.[partyName] = 'ARTEMIS COMERCIALIZACAO DE ENERGIA LTDA.' and A.[counterpartyName] = 'SAFIRA GESTAO E CONSULTORIA EM ENERGIA LTDA' then 'Negocio'
								WHEN SUBSTRING(A.Code, 1, 2) = 'IC' AND A.[partyName] = 'ARTEMIS COMERCIALIZACAO DE ENERGIA LTDA.' and A.[counterpartyName] = 'SAFIRA TRADING E GERACAO DE ENERGIA LTDA.' then 'Negocio'
								WHEN SUBSTRING(A.Code, 1, 2) = 'IC' AND A.[partyName] = 'SAFIRA TRADING E GERACAO DE ENERGIA LTDA.' and A.[counterpartyName] = 'ARTEMIS COMERCIALIZACAO DE ENERGIA LTDA.' then 'Negocio'
								WHEN SUBSTRING(A.Code, 1, 2) = 'IC' AND A.[partyName] = 'SAFIRA TRADING E GERACAO DE ENERGIA LTDA.' and A.[counterpartyName] = 'SAFIRA GESTAO E CONSULTORIA EM ENERGIA LTDA' then 'Negocio'
								WHEN SUBSTRING(A.Code, 1, 2) = 'IC' AND A.[partyName] = 'SAFIRA TRADING E GERACAO DE ENERGIA LTDA.' and A.[counterpartyName] = 'SAFIRA ADMINISTRACAO E COMERCIALIZACAO DE ENERGIA S.A.' then 'Negocio'
								WHEN SUBSTRING(A.Code, 1, 2) = 'IC' then 'IC'
								WHEN SUBSTRING(A.Code, 1, 3) = 'CLI' then 'Negocio'
								ELSE 'Negocio'
						END																																																									AS Negocio
						
								,CASE 
								WHEN (A.partyCNPJ=A.counterpartyCNPJ) THEN 'IC'
								WHEN (A.partyCNPJ<>A.counterpartyCNPJ) THEN 
																	CASE WHEN (a.counterpartyCNPJ IN ('09.495.582/0001-07','10.938.805/0001-34','11.482.752/0001-52','31.562.321/0001-03','32.312.466/0001-19','35.417.904/0001-00','38.349.131/0001-51')) THEN 'Negocio Interno'
																	ELSE 'Negocio Externo'  
																	END

						END                                                                                                                                                                                                                                 AS TipoNegocio
						
						
						
						,CASE
								WHEN A.[classifications] = 'ESTRATEGIA MESA' and (A.[userOperatorName] = 'Mikio Kawai Jr' or A.[userOperatorName] = 'Mikio Kawai') THEN 'Sfr'
								WHEN A.[classifications] = 'SPOT MESA'
									or A.[classifications] = 'GIRO RAPIDO' 
									or A.[classifications] = 'ESTRATEGIA MESA'  
									or A.[classifications] = 'Comercial Mesa'
									or A.[classifications] = 'Varejo'
									or A.[classifications] = 'Corporate' THEN 'Mesa'
								WHEN A.[classifications] = 'OP PF'  THEN 'PF'
								WHEN A.[classifications] = 'Varejo_Mesa'  THEN A.[classifications]
								WHEN A.[classifications] = 'Corporate_Mesa'  THEN A.[classifications]
								WHEN A.[classifications] = 'Operações_Estruturadas'  THEN 'Sfr'
								WHEN A.[classifications] = 'Manaus_Mesa'  THEN A.[classifications]
								Else 'Sfr'
						END																																																									AS Portfolio	
						,CONVERT([date],CONVERT([varchar](8),[year]*10000+[month]*100+1))																																									AS DataFornecimento
						,CONVERT(DATE,CONVERT([varchar](8),YEAR(CONVERT(DATE,A.[createdAt]))*10000+MONTH(CONVERT(DATE,A.[createdAt]))*100+1))																												AS MesCriacao	
						,DATEDIFF(m, CONVERT(DATE,A.[startDate]), CONVERT(DATE,A.[endDate])) + 1																																							AS DuracaoContrato	
						,'Safira'																																																							AS Thunders
						, A.portfolios																																																						AS portfolios	
						, A.basePriceWithReadjustment 																																																		AS PrecoContratoComReajuste			
						, @DataLogHistorico AS ProcessInsertTimeInic
					FROM Book.dbo.s_all_operations(@DataHistorico) AS A
					) TempTableAllHistory;


END



			SELECT
				  DataFornecimento
				, isTrading
				, isServices
				, isGeneration
				, Thunders
				, EmpresaResponsavel
				, TipoNegocio
				, Classificacao
				--, TipoContrato
				, TipoOperacao
				, NaturezaOperacao
				, Submercado
				, FonteEnergia
				, FlexibilidadePreco

				, SUM(VolumeFinal_MWh) AS VolumeFinal_MWh
				, SUM(VolumeFinal_MWm) AS VolumeFinal_MWm
				, SUM(VolumeFinal_MWh * PrecoContrato) / NULLIF( SUM(VolumeFinal_MWh),0) AS PrecoContrato
				, SUM(VolumeFinal_MWh * PrecoFinal) / NULLIF( SUM(VolumeFinal_MWh),0) AS PrecoFinal
				, SUM(VolumeFinal_MWh * Spread) / NULLIF( SUM(VolumeFinal_MWh),0) AS Spread
				, SUM(TetoPreco) AS TetoPreco
				, SUM(PisoPreco) AS PisoPreco
			FROM #TempTableAllHistory
			WHERE (CASE	WHEN FlexibilidadePreco = 'Fixo' THEN FlexibilidadePreco
									WHEN FlexibilidadePreco = 'Variável' THEN (CASE WHEN TetoPreco is null AND PisoPreco is null THEN FlexibilidadePreco --Variavel normal
																					 WHEN TetoPreco > 0 AND PisoPreco > 0 THEN 'Collar'
																					 WHEN TetoPreco > 0 OR PisoPreco > 0 THEN 'Opção'												 
																				  END )
										END)   IN ('Fixo','Variável')
				and DataFornecimento = '2023-03-01'
				and Classificacao = 'DIRECIONAL' --'Sfr2'
				and TipoNegocio = 'Negocio Externo' --'Negocio Interno'
				--and TipoContrato = 'Físico'
				and NaturezaOperacao = 'Venda' --'Compra'
				and Submercado = 'SE'
				and FonteEnergia = 'Convencional'
				and FlexibilidadePreco = 'Fixo' --'Variável';
				and BoletaAtiva = 1
				-- Remove as boletas apagadas
				AND UnidadeNegocio != 'Serviços'
			GROUP BY
				  DataFornecimento
				, isTrading
				, isServices
				, isGeneration
				, Thunders
				, EmpresaResponsavel
				, TipoNegocio
				, Classificacao
				, TipoOperacao
				, NaturezaOperacao
				
				, Submercado
				, FonteEnergia
				, FlexibilidadePreco

			UNION ALL
	 
			-- Esse select considera apenas o COLLAR e a OPCAO, serve para ajudar no VaR, já que será por contrato.
			SELECT
				 DataFornecimento
				, isTrading
				, isServices
				, isGeneration
				, Thunders
				, EmpresaResponsavel
				, TipoNegocio
				, Classificacao
				, TipoOperacao
				, NaturezaOperacao
				, Submercado
				, FonteEnergia
				, FlexibilidadePreco
				, VolumeFinal_MWh AS VolumeFinal_MWh
				, VolumeFinal_MWm AS VolumeFinal_MWm
				, PrecoContrato AS PrecoContrato
				, PrecoFinal  AS PrecoFinal
				, Spread AS Spread
				, TetoPreco
				, PisoPreco
			FROM #TempTableAllHistory
			WHERE (CASE	WHEN FlexibilidadePreco = 'Fixo' THEN FlexibilidadePreco
									WHEN FlexibilidadePreco = 'Variável' THEN (CASE WHEN TetoPreco is null AND PisoPreco is null THEN FlexibilidadePreco --Variavel normal
																					 WHEN TetoPreco > 0 AND PisoPreco > 0 THEN 'Collar'
																					 WHEN TetoPreco > 0 OR PisoPreco > 0 THEN 'Opção'												 
																				  END )
										END)  IN ('Collar','Opção')

--SELECT top 100 * FROM #TempTableAllHistory
				and DataFornecimento = '2023-03-01'
				and Classificacao = 'DIRECIONAL' --'Sfr2'
				and TipoNegocio = 'Negocio Externo' --'Negocio Interno'
				--and TipoContrato = 'Físico'
				and NaturezaOperacao = 'Venda' --'Compra'
				and Submercado = 'SE'
				and FonteEnergia = 'Convencional'
				and FlexibilidadePreco = 'Fixo' --'Variável';
				and BoletaAtiva = 1
				-- Remove as boletas apagadas
				AND UnidadeNegocio != 'Serviços'


