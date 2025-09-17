#!/bin/bash
> /home/administrador/projetos/processos_sql_server/running_scripts/bases_resultado/run_information_dquarted_thunders.txt

# Activa el entorno virtual
source /home/administrador/projetos/processos_sql_server/.venv/bin/activate

cd /home/administrador/projetos/processos_sql_server/src/v1

python /home/administrador/projetos/processos_sql_server/src/v1/run_information_dquarted_thunders.py

#C:\Users\abarbosa\Documents\projetos\processos_sql_server\src\run_cthird_bases_resultado_paralelo.py
#/home/administrador/projetos/processos_sql_server/running_scripts/bases_portifolio/run_cthird_bases_resultado.sh