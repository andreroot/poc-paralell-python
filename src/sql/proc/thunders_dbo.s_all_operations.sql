USE [BookComercial]
GO
/****** Object:  UserDefinedFunction [dbo].[s_all_operations]    Script Date: 28/02/2025 15:34:39 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





ALTER function [dbo].[s_all_operations] (
    @dataVigencia VARCHAR(100)
) RETURNS TABLE 
AS

RETURN
WITH validFromSelect AS (

    SELECT *,
           CASE
               WHEN syncCreatedAt IS NOT NULL THEN syncCreatedAt
               WHEN syncDeletedAt IS NOT NULL THEN syncDeletedAt
               ELSE syncUpdatedAt
               END AS validFrom

    FROM operation_history oh),

     validFromLead AS (
         SELECT *, row_number() OVER (PARTITION BY id, year, month, sequence,businessUnitDescription ORDER BY validFrom) AS rn
         FROM validFromSelect
     )

select * from (

SELECT vl.*, vl2.validFrom AS validTo
FROM validFromLead vl
         LEFT JOIN validFromLead vl2
                   ON vl.rn = vl2.rn - 1 AND vl.id = vl2.id AND vl.year = vl2.year AND vl.month = vl2.month AND vl.sequence = vl2.sequence and vl.businessUnitDescription = vl2.businessUnitDescription

) res 
WHERE 
((validFrom < @dataVigencia and validTo > @dataVigencia) or (validFrom < @dataVigencia and validTo is null))
AND syncDeletedAt is null

