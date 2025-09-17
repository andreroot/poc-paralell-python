		-------------------------------------------------------------------------------------------------------------------------------------------
							-- Merge para inserir em uma tabela física
		-------------------------------------------------------------------------------------------------------------------------------------------

	--IF OBJECT_ID('book.[HistoricoResultado_log]', 'U') IS NOT NULL
	
	--MERGE 
	--	Book.book.[HistoricoResultado_log] AS Destino
	--USING 
	--	#MyTable  AS Origem 
	
	--ON CONVERT(date,Origem.DataHistorico)  = Destino.DataHistorico
	--		AND Origem.DataFornecimento = Destino.DataFornecimento
	--		AND (Origem.Classificacao collate SQL_Latin1_General_CP1_CI_AS)  = (Destino.Classificacao collate SQL_Latin1_General_CP1_CI_AS)

	--WHEN MATCHED THEN
	--	UPDATE 
	--	SET 
	--	 destino.VolumeFinal_MWh = Origem.VolumeFinal_MWh
	--	,destino.VolumeFinal_MWm = Origem.VolumeFinal_MWm
	--	,destino.Resultado		 = Origem.Resultado


 --Registro não existe no destino. Vamos inserir.
	--WHEN NOT MATCHED THEN
	--	INSERT 
	--	VALUES(Origem.DataHistorico				
	--		, Origem.DataFornecimento			
	--		, Origem.NaturezaOperacao    		
	--		, Origem.FonteEnergia				
	--		, Origem.Submercado				
	--		, Origem.Thunders					
	--		, Origem.Classificacao		
	--		, Origem.FlexibilidadePreco		
	--		, Origem.TipoFlexibilidadePreco	
	--		, Origem.VolumeFinal_MWh			
	--		, Origem.VolumeFinal_MWm			
	--		, Origem.PrecoContrato				
	--		, Origem.PrecoFinal				
	--		, Origem.Spread					
	--		, Origem.TetoPreco					
	--		, Origem.PisoPreco					
	--		, Origem.Preco						
	--		, Origem.Curva						
	--		, Origem.Preco_PLD					
	--		, Origem.Preco_Energia				
	--		, Origem.Resultado 	
	--		);