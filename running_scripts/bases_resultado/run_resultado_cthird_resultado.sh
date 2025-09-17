#!/bin/sh

# #> /home/n8n/projetos/github/processos_sql_server/running_scripts/bases_resultado/run_resultado_cthird_resultado.txt
# > /home/node/github/processos_sql_server/running_scripts/bases_resultado/run_resultado_cthird_resultado.txt

# Activa el entorno virtual
source /home/node/github/processos_sql_server/.venv/bin/activate

cd /home/node/github/processos_sql_server/src/v1

python /home/node/github/processos_sql_server/src/v1/run_resultado_cthird_resultado_lambda.py

#C:\Users\abarbosa\Documents\projetos\processos_sql_server\src\run_cthird_bases_resultado_paralelo.py
#/home/administrador/projetos/processos_sql_server/running_scripts/bases_portifolio/run_cthird_bases_resultado.sh