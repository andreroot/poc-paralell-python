BEGIN

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

DECLARE @DataInicio as varchar(100);
DECLARE @DataFim as varchar(100);
DECLARE @Curva     VARCHAR(250);

DECLARE @n_datas AS INT = 2;
DECLARE @it AS INT = 1;

	BEGIN
	WHILE @it < @n_datas
		BEGIN
		-- SET @DataFim = (select  DATEADD(DAY, -@it, max(data)) start_date from book.curva.Curva_Fwd where curva = 'Oficial');
		-- SET @DataInicio = (select  DATEADD(DAY, 0, max(data)) start_date from book.curva.Curva_Fwd where curva = 'Oficial' and data < @DataFim);
		SET @DataInicio = (select  DATEADD(DAY, 0, max(data)) start_date from book.curva.Curva_Fwd where curva = 'Oficial');
		SET @DataFim = (select  DATEADD(DAY, @it, max(data)) start_date from book.curva.Curva_Fwd where curva = 'Oficial');		
		SET @Curva = 'Oficial'

		EXEC BOOK.[STP_HistoricoResultado_log] @DataInicio, @DataFim, @Curva;
		
		SET @it = @it + 2;

		END;

	END;

END;






