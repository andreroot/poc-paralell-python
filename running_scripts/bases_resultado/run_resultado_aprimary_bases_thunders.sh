#!/bin/bash
> /home/administrador/projetos/processos_sql_server/running_scripts/bases_resultado/run_resultado_aprimary_bases_thunders.txt

# Activa el entorno virtual
source /home/administrador/projetos/processos_sql_server/.venv/bin/activate

cd /home/administrador/projetos/processos_sql_server/src/v1

python /home/administrador/projetos/processos_sql_server/src/v1/run_resultado_aprimary_bases_thunders.py

#C:\Users\abarbosa\Documents\projetos\processos_sql_server\src\run_aprimary_bases_portifolio_paralelo.py

#sudo chown -R andre:administrador /home/administrador/projetos/geralog_fluxo_caixa/running_scripts/
#sudo chown administrador /home/administrador/projetos/geralog_fluxo_caixa/running_scripts/