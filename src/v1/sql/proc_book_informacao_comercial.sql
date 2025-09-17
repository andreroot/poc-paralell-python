BEGIN

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

DECLARE @DataInicio as varchar(100);
DECLARE @Curva     VARCHAR(250);

DECLARE @n_datas AS INT = 2;
DECLARE @it AS INT = 0;

-- SET @DataInicio = (select  DATEADD(DAY, 0, max(data)) start_date from book.curva.Curva_Fwd where curva = 'Oficial');
-- SET @Curva = 'Oficial'

-- exec [Book].[InformacaoComercial] @DataInicio, @Curva;

	BEGIN
	WHILE @it < @n_datas
		BEGIN
		SET @DataInicio = (select  DATEADD(DAY, -@it, max(data)) start_date from book.curva.Curva_Fwd where curva = 'Oficial');

		SET @Curva = 'Oficial'

		exec [Book].[InformacaoComercial] @DataInicio, @Curva;

		SET @it = @it + 1;

		END;

	END;
	
--use book;
GRANT SELECT ON OBJECT:: book.book.proc_InformacaoComercial_table5 TO [SAFIRA\jonathas.olsen];
END;