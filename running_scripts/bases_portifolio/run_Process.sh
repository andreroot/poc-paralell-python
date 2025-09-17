#!/bin/bash

# Activa el entorno virtual
source /home/administrador/projetos/processos_sql_server/.venv/bin/activate

> /var/log/cron.log

NOME_PYHTON=$1

NOME_PYHTON_LOG=$1
NOME_PYHTON_LOG+="_log"

echo $NOME_PYHTON

> /home/administrador/projetos/processos_sql_server/running_scripts/bases_portifolio/$NOME_PYHTON_LOG.txt

cd /home/administrador/projetos/processos_sql_server/src/v1/

# hh_execução   python
# 10 1 * * *    run_portifolio_aprimary_operation_history
# 0 4 * * *     run_portifolio_bsecond_bases_historico

python /home/administrador/projetos/processos_sql_server/src/v1/$NOME_PYHTON.py


exit