select count(1), max(DataInsertTable)  from book.APIPayments.Payments
select count(1), max(DataInsertTable) from bookcomercial.APIPayments.Payments


use Treinamento;

/**/
use Treinamento;

select * from lkok.MeasuringPoint; --where id = '45f5a16f-4f47-4b60-a891-9a3c11d767f1';

select * from lkok.MeasuringPoint_details; --where id = '45f5a16f-4f47-4b60-a891-9a3c11d767f1';

select * from lkok.MeasuringPoint_associatedAssets;


select  count(1), MAX(DataInsertTable) 
from TREINAMENTO.lkok.MeasuringAdjust;

select  count(1), MAX(DataInsertTable)
from TREINAMENTO.lkok.MeasuringProjectionConsolidateMonthYear; 
--where id = '45f5a16f-4f47-4b60-a891-9a3c11d767f1' and "consumptionPeak.projected" is not null;

select  count(1), MAX(DataInsertTable)
from TREINAMENTO.lkok.MeasuringAdjustConsolidated; 
--where id = '45f5a16f-4f47-4b60-a891-9a3c11d767f1' ;--and "consumptionPeak.projected" is not null;
/**/