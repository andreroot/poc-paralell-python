
use Modelo;

SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = N'ThundersAPIACLProposals'

select * from  Modelo.ThundersAPIACLProposals.proposal

SELECT s.name as schema_name, t.name as table_name, c.name
FROM sys.columns AS c
INNER JOIN sys.tables AS t ON t.object_id = c.object_id
INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
WHERE t.name = 'operation_history' --t.name = 'operation' 
AND s.name = 'dbo'
AND c.name = 'totalVolumeMWm';
--totalVolumeMWm


CREATE TABLE lkok.[MeasuringAdjustConsolidated] (
        reference DATETIME NULL, 
        id VARCHAR(100) NULL, 
        [consumptionPeak.adjusted] FLOAT NULL, 
        [consumptionOffPeak.adjusted] FLOAT NULL, 
        [demandPeak.adjusted] FLOAT NULL, 
        [demandOffPeak.adjusted] FLOAT NULL, 
        [demandSeasonalityPeak.adjusted] FLOAT NULL, 
        [demandSeasonalityOffPeak.adjusted] FLOAT NULL, 
        [activeConsumption.adjusted] FLOAT NULL, 
        [consumptionPeak.measured] FLOAT NULL, 
        [consumptionOffPeak.measured] FLOAT NULL, 
        [demandPeak.measured] FLOAT NULL, 
        [demandOffPeak.measured] FLOAT NULL, 
        [activeGeneration.measured] FLOAT NULL, 
        [activeConsumption.measured] FLOAT NULL, 
        [reactiveGeneration.measured] FLOAT NULL, 
        [reactiveConsumption.measured] FLOAT NULL, 
        [reactiveDemandOffPeak.measured] FLOAT NULL, 
        [reactiveDemandPeak.measured] FLOAT NULL, 
        [reactiveExcessConsumptionOffPeak.measured] FLOAT NULL, 
        [reactiveExcessConsumptionPeak.measured] FLOAT NULL, 
        [testActiveGeneration.measured] FLOAT NULL, 
        [testReactiveGeneration.measured] FLOAT NULL, 
        [higherHourlyConsumption.measured] FLOAT NULL, 
        [reactiveExcessConsumptionOffPeak.adjusted] FLOAT NULL, 
        [reactiveExcessConsumptionPeak.adjusted] FLOAT NULL, 
        [activeGeneration.adjusted] FLOAT NULL, 
        [reactiveGeneration.adjusted] FLOAT NULL, 
        [reactiveConsumption.adjusted] FLOAT NULL, 
        [reactiveDemandPeak.adjusted] FLOAT NULL, 
        [testActiveGeneration.adjusted] FLOAT NULL, 
        [testReactiveGeneration.adjusted] FLOAT NULL, 
        [reactiveDemandOffPeak.adjusted] FLOAT NULL, 
        [DataInsertTable] VARCHAR(max) NULL
)