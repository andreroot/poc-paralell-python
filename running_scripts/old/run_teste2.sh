#!/bin/bash

# Constantes ALTERAR PATH PARA SER EXECUTADO COMO administrador  /home/administrador/projetos/

FILE_NOVO=/home/administrador/projetos/processos_sql_server/running_scripts/receita_novo2.html
FILE_OLD=/home/administrador/projetos/processos_sql_server/running_scripts/receita2.html
SITE="https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj/"
FILE_LOG="/home/administrador/projetos/processos_sql_server/running_scripts/arquivos_receita_dia2.txt"

valida_arquivo_html_existe(){

    #FILE=/home/administrador/projetos/pipeline-site-receita-s3/script/monitoria/receita.html

    if [ -e "$FILE_OLD" ] ; then
    
        echo "Arquivo html ja existe:"$FILE_OLD

        # MARCAR DATA DA ULTIMA VEZ QUE FOI BAIXADO OS ARQUIVOS DO SITE DA RECEITA
        DATA_ARQUIVO_ATUAL=$(echo $(sed -n '12p' $FILE_OLD | head -1) | sed 's/\(<tr><td valign="top"><img src="\/icons\/compressed.gif" alt="\[ \]"><\/td><td>\)\(<a href="\.*[a-zA-Z]*\.\zip\">[a-zA-Z]*\.\zip<\/a>\)\(.<\/td><td align="right">\)\([0-9]\{4\}\-[0-9]\{2\}\-[0-9]\{2\}\)\(.*\)/\4/')

        #DATA_ARQUIVO É A DATA ANTIGA DOS ARQUIVOS CASO TENHA ARQUVOS NOVOS DEVE SER MENOR
        DATA_ARQUIVO_ATUAL_TRD=$(date -d "$DATA_ARQUIVO_ATUAL" +%Y-%m-%d)

        echo "ULTIMA DATA: "$DATA_ARQUIVO_ATUAL_TRD

        main $DATA_ARQUIVO_ATUAL_TRD

    else
        # PROCESSAR ARQUIVO PARA NOVA DATA DOS ARQUIVOS
        echo "Criar arquivo html:"$FILE_OLD

        # INCLUIR DATA DA PAGINA DO MES ANTERIOR AO ATUAL
        # wget $SITE$(date -d "-1 months" +"%Y-%m")"/" -O $FILE_OLD
        wget $SITE$(date +"%Y-%m")"/" -O $FILE_OLD

        # MARCAR DATA DA ULTIMA VEZ QUE FOI BAIXADO OS ARQUIVOS DO SITE DA RECEITA
        DATA_ARQUIVO_ATUAL=$(echo $(sed -n '12p' $FILE_OLD  | head -1) | sed 's/\(<tr><td valign="top"><img src="\/icons\/compressed.gif" alt="\[ \]"><\/td><td>\)\(<a href="\.*[a-zA-Z]*\.\zip\">[a-zA-Z]*\.\zip<\/a>\)\(.<\/td><td align="right">\)\([0-9]\{4\}\-[0-9]\{2\}\-[0-9]\{2\}\)\(.*\)/\4/')

        #DATA_ARQUIVO É A DATA ANTIGA DOS ARQUIVOS CASO TENHA ARQUVOS NOVOS DEVE SER MENOR
        DATA_ARQUIVO_ATUAL_TRD=$(date -d "$DATA_ARQUIVO_ATUAL" +%Y-%m-%d)

        echo "ULTIMA DATA: "$DATA_ARQUIVO_ATUAL_TRD

        #main $DATA_ARQUIVO_ATUAL_TRD

    fi
}

valida_arquivo_html_existe