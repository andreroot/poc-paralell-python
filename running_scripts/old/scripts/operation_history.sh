#!/bin/bash
# 0 7 * * * /home/administrador/projetos/processos_sql_server/scripts/operation_history.sh

PROJECT_PATH=/home/administrador/projetos/processos_sql_server
PIPELINE_NAME=operation_history
source "$PROJECT_PATH/.venv/bin/activate"
cd $PROJECT_PATH
python "$PROJECT_PATH/scripts/${PIPELINE_NAME}.py" > "$PROJECT_PATH/data/logs/${PIPELINE_NAME}_$(date +\%Y\%m\%d_\%H\%M\%S).log" 2>&1
