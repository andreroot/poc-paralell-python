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
            , descricao_status_alt descricao_status
            , DATE_FORMAT(date_end, '%Y_%m_%d') date_end_format
            --, max(date_start) OVER xlog max_date_start
            , date_start
            --, MAX(date_end) OVER xlog max_date_end
            , date_end
            --, RANK() OVER xlog nrows_etapa
FROM add_diff_timesatmp
/**filtro de processos finalizado*/
WHERE status != 'EXECUTANDO' 
AND date_end >= date_parse('2025-02-06 00:00:00.000', '%Y-%m-%d %T.%f') 
)

SELECT * FROM ultimo_processo_geralog
