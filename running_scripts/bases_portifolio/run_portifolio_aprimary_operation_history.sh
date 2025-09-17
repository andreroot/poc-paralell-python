#!/bin/bash
# 0 7 * * * /home/administrador/projetos/processos_sql_server/running_scripts/bases_operation_history/run_primary_base_operation_hist.sh

# PROJECT_PATH=/home/administrador/projetos/processos_sql_server
# PIPELINE_NAME=run_aprimary_operation_history
# source "$PROJECT_PATH/.venv/bin/activate"
# cd $PROJECT_PATH
# python "$PROJECT_PATH/src/v1/portifolio/${PIPELINE_NAME}.py" > "$PROJECT_PATH/data/logs/${PIPELINE_NAME}_$(date +\%Y\%m\%d_\%H\%M\%S).log" 2>&1


#!/bin/bash
> /home/administrador/projetos/processos_sql_server/running_scripts/bases_portifolio/run_portifolio_aprimary_operation_history.txt

# Activa el entorno virtual
source /home/administrador/projetos/processos_sql_server/.venv/bin/activate

cd /home/administrador/projetos/processos_sql_server/src/v1

python /home/administrador/projetos/processos_sql_server/src/v1/run_portifolio_aprimary_operation_history.py

