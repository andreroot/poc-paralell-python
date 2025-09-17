#!/bin/bash
> /home/administrador/projetos/processos_sql_server/running_scripts/run_comercial.txt

# Activa el entorno virtual
source /home/administrador/projetos/processos_sql_server/.venv/bin/activate

python /home/administrador/projetos/processos_sql_server/src/main_comercial.py


#sudo chown -R andre:administrador /home/administrador/projetos/geralog_fluxo_caixa/running_scripts/
#sudo chown administrador /home/administrador/projetos/geralog_fluxo_caixa/running_scripts/