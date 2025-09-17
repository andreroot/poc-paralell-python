WITH add_diff_timesatmp as 
(
SELECT *           
, CASE WHEN descricao_status IS NOT NULL THEN
            descricao_status
            ELSE status END descricao_status_alt 
, ROUND(CAST(date_diff('millisecond', date_start, date_end)  AS DOUBLE) , 3)/ 1000 tempo_excucao_ok
FROM "safira-stream-database"."tab_stream_process_monitoria" 
--WHERE DATE_FORMAT(date_end,'%Y-%m-%d') = '2024-02-23'
)

,ultimo_processo_geralog AS 
(SELECT id_process_name
            , process_name_pai
            , tempo_excucao_ok
            , status
            , DATE_FORMAT(date_start, '%Y_%m_%d') date_start_format
            --, descricao_status_alt descricao_status
            , DATE_FORMAT(date_end, '%Y_%m_%d') date_end_format
            , max(date_start) OVER xlog max_date_start
            , MAX(date_end) OVER xlog max_date_end
            , RANK() OVER xlog nrows_etapa
FROM add_diff_timesatmp
/**filtro de processos em execução*/
WHERE status = 'EXECUTANDO' 
AND  process_name_pai like 'sql-server/extract_%'
AND date_start >= date_parse('2025-02-07 00:00:00.000', '%Y-%m-%d %T.%f') 
/** para filtrar processo especifico para validação */
--WHERE  process_name_pai like 'extract_%'
--AND process_name_pai like 'sql-server%'
/**/
--WHERE status != 'EXECUTANDO' 
--AND date_end >= date_parse('2025-02-06 00:00:00.000', '%Y-%m-%d %T.%f') 
WINDOW xlog AS (PARTITION BY  id_process_name, process_name_pai, process_name, step_name, date_format(date_end, '%Y_%m_%d'), status, DATE_FORMAT(date_start, '%Y_%m_%d')  ORDER BY date_end DESC)

/** para filtrar processo especifico para validação *
AND process_name_pai in ('dados/api/pipeline-agendor-raw')
**/
--AND process_name_pai in ('ehub_post_api','insert_negocio_api_ehub')
/** para filtrar processo especifico para validação
--AND date_end BETWEEN date_parse('2024-01-01 00:00:00.000', '%Y-%m-%d %T.%f') AND  date_parse('2024-02-20 00:00:00.000', '%Y-%m-%d %T.%f')
**/
)

SELECT * FROM ultimo_processo_geralog
/**
SELECT ult.process_name_pai
, ult.id_process_name 
, ori.process_name
, ori.step_name
, ult.descricao_status
, ori.status
, ult.max_date_end date_end
, date_format(max_date_end , '%Y-%m-%d') date_end_format
, date_format(max_date_end , '%H') time_end_format
, ori.date_start
, ult.tempo_excucao_ok
-- , regexp_extract(ult.descricao_status,'\Response 204') 
-- , regexp_extract(ult.descricao_status,'\<\Response \[204]\>') 
-- , regexp_extract(ori.status,'\OK')
-- , CASE WHEN (date_end  < date_parse('2024-02-24 00:00:00.000', '%Y-%m-%d %T.%f') and ult.process_name_pai = 'ehub_post_api' AND regexp_extract(ult.descricao_status,'\Response 204') IS NULL AND regexp_extract(ult.descricao_status,'\<\Response \[204]\>') IS NULL)   THEN
--             'ERROR'
--       WHEN (date_end  < date_parse('2024-02-24 00:00:00.000', '%Y-%m-%d %T.%f') and ult.process_name_pai != 'ehub_post_api' AND regexp_extract(ult.descricao_status,'\OK') IS NULL and regexp_extract(ori.status,'\OK') IS NULL)   THEN
--             'ERROR'
--       WHEN (date_end  < date_parse('2024-02-24 00:00:00.000', '%Y-%m-%d %T.%f')  and ult.process_name_pai != 'ehub_post_api' AND (regexp_extract(ult.descricao_status,'\OK') IS NOT NULL OR regexp_extract(ori.status,'\OK') IS NOT NULL ))   THEN
--             'OK'      
--       WHEN (date_end  < date_parse('2024-02-24 00:00:00.000', '%Y-%m-%d %T.%f')  and ult.process_name_pai = 'ehub_post_api' AND (regexp_extract(ult.descricao_status,'\Response 204') IS NOT NULL OR regexp_extract(ult.descricao_status,'\<\Response \[204]\>')  IS NOT NULL ))   THEN
--             'OK'               
--   ELSE  ori.status END valida_erro
, CASE 
      WHEN ((regexp_extract(ult.descricao_status,'\ERROR') IS NOT NULL) 
      or (regexp_extract(ori.status,'\ERROR') IS NOT NULL)
      or (regexp_extract(ori.descricao_status,'\Erro') IS NOT NULL)
      or (regexp_extract(ori.status,'\Erro') IS NOT NULL) 
      or (regexp_extract(ult.descricao_status,'\Response 400') IS NOT NULL) 
      or  (regexp_extract(ult.descricao_status,'\<\Response \[400]\>') IS NOT NULL)  
      or (regexp_extract(ult.descricao_status,'\Response 401') IS NOT NULL) 
      or  (regexp_extract(ult.descricao_status,'\<\Response \[401]\>') IS NOT NULL)) THEN
            'ERROR'
      WHEN ((regexp_extract(ult.descricao_status,'\Response 204') IS NOT NULL) 
            OR (regexp_extract(ult.descricao_status,'\Response 200') IS NOT NULL) 
            OR (regexp_extract(ult.descricao_status,'\<\Response \[204]\>')  IS NOT NULL ) 
            OR (regexp_extract(ult.descricao_status,'\<\Response \[200]\>')  IS NOT NULL )
            or (regexp_extract(ult.descricao_status,'\OK') IS NOT NULL) 
            OR (regexp_extract(ori.status,'\OK') IS NOT NULL )
            )   THEN
            'OK'               
  ELSE  ori.status END valida_erro
  
FROM ultimo_processo_geralog ult
INNER JOIN add_diff_timesatmp ori ON (ori.id_process_name = ult.id_process_name 
                                    and ori.date_end = ult.max_date_end)  
--WHERE ult.nrows_etapa=1      
**/  


select top 10 *--, LEFT(ProcessInsertTimeInic, 10) ProcessInsertTimeInic_ALT
from [modelo].[BaseHistorica].[LogBoletasProcessadas] 
--WHERE Thunders = 'Comercial'
--order by ProcessInsertTimeInic desc
WHERE LEFT(ProcessInsertTimeInic, 10)  = (SELECT CONVERT(DATE, DATEADD(DAY, 0, GETDATE()), 103))