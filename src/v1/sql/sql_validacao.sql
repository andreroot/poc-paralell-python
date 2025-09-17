SELECT CONVERT(date,ProcessInsertTimeInic, 103), count(1)
FROM  [modelo].[BaseHistorica].[BoletasProcessadasv2] (NOLOCK)
WHERE CONVERT(date,ProcessInsertTimeInic, 103) >= DATEADD(DAY,-1,GETDATE()) --  DATEADD(MONTH,-6,GETDATE())
GROUP BY CONVERT(DATE,ProcessInsertTimeInic, 103)

	
SELECT CONVERT(date,ProcessInsertTimeInic, 103),  count(1)
FROM  [modelo].[BaseHistorica].[BoletasProcessadasv2] (NOLOCK)
WHERE CONVERT(date,ProcessInsertTimeInic, 103) < DATEADD(DAY,-15,GETDATE()) --  DATEADD(MONTH,-6,GETDATE())
GROUP BY CONVERT(DATE,ProcessInsertTimeInic, 103)

DELETE FROM [modelo].[BaseHistorica].[BoletasProcessadasv2] 
--WHERE Thunders = 'Indra'
WHERE CONVERT(date,ProcessInsertTimeInic, 103) < DATEADD(DAY,-15,GETDATE()) --  DATEADD(MONTH,-6,GETDATE())
