#!/bin/bash

# Activa el entorno virtual
source /home/administrador/projetos/processos_sql_server/.venv/bin/activate

> /var/log/cron.log

NOME_PYHTON=$1

NOME_PYHTON_LOG=$1
NOME_PYHTON_LOG+="_log"

echo $NOME_PYHTON

> /home/administrador/projetos/processos_sql_server/running_scripts/bases_resultado/$NOME_PYHTON_LOG.txt

cd /home/administrador/projetos/processos_sql_server/src/v1/

# hh_execução   python
# 10 1 * * *    run_resultado_aprimary_bases_thunders
# 0 4 * * *     run_resultado_bsecond_posicao
# 0 4 * * *     run_resultado_cthird_resultado
# 0 4 * * *     run_information_dquarted_thunders


python /home/administrador/projetos/processos_sql_server/src/v1/$NOME_PYHTON.py


exit