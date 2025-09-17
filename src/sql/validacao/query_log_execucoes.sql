

    SELECT *, CONVERT(varchar(19), ProcessInsertTimeInic)
    FROM  [modelo].[BaseHistorica].[LogBoletasProcessadas]
    WHERE CONVERT(varchar(19), ProcessInsertTimeInic) >= (select concat(CONVERT(DATE, DATEADD(DAY, -6, GETDATE()), 103),'T23:59:59.000Z'))

