#!/bin/bash
# 0 7 * * * /home/administrador/projetos/processos_sql_server/scripts/running_scripts/bases_operation_history/run_second_bases_historico_portfolio.sh

#!/bin/bash
> /home/administrador/projetos/processos_sql_server/running_scripts/bases_portifolio/run_portifolio_bsecond_bases_historico.txt

# Activa el entorno virtual
source /home/administrador/projetos/processos_sql_server/.venv/bin/activate

cd /home/administrador/projetos/processos_sql_server/src/v1

python /home/administrador/projetos/processos_sql_server/src/v1/run_portifolio_bsecond_bases_historico.py