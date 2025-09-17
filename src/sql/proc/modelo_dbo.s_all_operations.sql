USE [Modelo]
GO
/****** Object:  UserDefinedFunction [dbo].[s_all_operations]    Script Date: 05/03/2025 10:52:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER function [dbo].[s_all_operations] (
    @dataVigencia DATETIME2
) RETURNS TABLE 
AS

RETURN
SELECT * FROM Modelo.[dbo].[proc_operation_history]
WHERE @dataVigencia BETWEEN ValidFrom AND ValidTo
AND IsSyncDeletedAtNull = 1
