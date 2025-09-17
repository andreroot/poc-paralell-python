#!/bin/bash
> /home/administrador/projetos/processos_sql_server/running_scripts/run_process.txt

# Activa el entorno virtual
source /home/administrador/projetos/processos_sql_server/.venv/bin/activate

python /home/administrador/projetos/processos_sql_server/src/main_process.py