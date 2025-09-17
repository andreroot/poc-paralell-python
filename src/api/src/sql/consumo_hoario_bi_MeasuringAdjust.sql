with modulacao as (
select  
B.subMarket
, B.energySource
, CASE WHEN energySource  = '1' THEN 'Convencional' 
      WHEN energySource  = '2' THEN 'Incentivada 50%' 
      WHEN energySource  = '3' THEN 'Incentivada 100%' 
      WHEN energySource  = '4' THEN 'Não Mapeado' 
      WHEN energySource  = '5' THEN 'Não Mapeado' 
      WHEN energySource  = '6' THEN 'Cogeração Qualificada 50%' 
      ELSE 'Não Programado'
  END as energySourceDescription

, CASE WHEN subMarket  = '1' THEN 'Sudeste' 
      WHEN subMarket  = '2' THEN 'Sul' 
      WHEN subMarket  = '3' THEN 'Nordeste' 
      WHEN subMarket  = '4' THEN 'Norte' 
      ELSE 'Não Programado'
  END as subMarketDescription

, B.parent_code code
, 'Economia Garantida' as operationType
, month(A.[period]) as mes
, CAST(A.[period] AS DATE) dt
, DATEPART(hour, A.[period]) hora
, A.activeConsumption Kwh
, A.activeConsumption/1000 Mwh

from TREINAMENTO.lkok.MeasuringAdjust A
left join [TREINAMENTO].[lkok].[guaranteed_savings_charges_services_physicalAssets] B
on A.physicalAssetId = B.physicalAssetId

--where A.physicalAssetId in ('82f1e432-8237-40de-a4f3-5b5e1207c738','d190fba4-72b2-4dc0-b41f-588ed8391f85')

--where CAST(A.[period] AS DATE) = '2025-01-09'
)


,calc_mod as (
select code, submarketDescription, energySourceDescription, mes, sum(Mwh) TotalMwhMes, count(1) totalhorasmes, sum(Mwh)/count(1) MedMwhMes
from modulacao
--WHERE code = 'EG009-24'
group by code, submarketDescription, energySourceDescription, mes
)



,result as (
SELECT A.*, B.TotalMwhMes, B.MedMwhMes,  totalhorasmes, B.MedMwhMes - A.Mwh as MwhDif
from modulacao A
INNER JOIN calc_mod B
on (A.code = B.code AND A.submarketDescription = B.submarketDescription AND A.energySourceDescription = B.energySourceDescription)

)


,calc_mod_pld as (


select Z.*,
Z.MedMwhMes*Z.Value CustoMedioPadrao, 
(Z.MedMwhMes*Z.Value)/Z.TotalMwhMes CustoPLDdiaMedioPadrao,

Z.Mwh*Z.Value CustoPadrao,
(Z.Mwh*Z.Value)/Z.TotalMwhMes CustoPLDdiaPadrao,

Z.MwhDif*Z.Value CustoModulacao,
(Z.MwhDif*Z.Value)/Z.TotalMwhMes CustoPLDdiaModulacao

from (
select code,  
submarketDescription, 
energySourceDescription, 
A.dt, 
A.hora ,  
A.Kwh,  
CASE WHEN A.Mwh = 0 THEN 1 WHEN A.Mwh = NULL THEN 1 ELSE A.Mwh END Mwh,
CASE WHEN A.MwhDif = 0 THEN 1 WHEN A.MwhDif = NULL THEN 1 ELSE A.MwhDif END MwhDif,
CASE WHEN A.TotalMwhMes = 0 THEN 1 WHEN A.TotalMwhMes = NULL THEN 1 ELSE A.TotalMwhMes END TotalMwhMes,
CASE WHEN A.MedMwhMes = 0 THEN 1 WHEN A.MedMwhMes = NULL THEN 1 ELSE A.MedMwhMes END MedMwhMes,
CASE WHEN B.Value = 0 THEN 1 WHEN B.Value = NULL THEN 1 ELSE B.Value END Value

from result A


left join TREINAMENTO.lkok.precos_horario B
on A.dt = B.date
and A.submarketDescription = B.Submercado
and A.hora = B.Hora

left join TREINAMENTO.lkok.precos_mensal C
on month(A.dt) = month(C.MES)
and year(A.dt) = year(C.MES)
and A.submarketDescription = C.submarket
) Z
)

select distinct *
from calc_mod_pld A
--from result
