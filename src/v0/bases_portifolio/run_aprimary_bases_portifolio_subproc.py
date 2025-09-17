import datetime

import sys
import subprocess
import platform

if __name__=='__main__':
    
    print(datetime.datetime.now())
    # C:\Users\abarbosa\Documents\projetos\processos_sql_server\src\bases_portifolio
    arquivos = ['main_comercial.py','main_indra.py','main_safira.py']
    processos = []
    output = []
    error = []
    so = platform.system()
    print(so)
    
            
    for arquivo in arquivos:
        if so == 'Windows':
            processo = subprocess.Popen(["C:\\Users\\abarbosa\\Documents\\projetos\\processos_sql_server\\.venv\\Scripts\\python.exe", f"C:\\Users\\abarbosa\\Documents\\projetos\\processos_sql_server\\src\\bases_portifolio\\{arquivo}"], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        else:
            processo = subprocess.Popen(["/home/administrador/projetos/processos_sql_server/.venv/bin/python3.11", f"/home/administrador/projetos/processos_sql_server/src/bases_portifolio/{arquivo}"], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        
        processos.append(processo)
            
    exit_codes = [processo.communicate() for processo in processos]
    
    #print(output.decode("utf-8"))
    ## OUTPUT 1
    output=str(exit_codes[0][0].decode("utf-8"))
    
    if output:
        print("executou com SUCESSO!!!")
        print(output)
    else:
        err=str(exit_codes[0][1].decode("utf-8"))
        print("executou com ERRO!!!")
        print(err)

    ## OUTPUT 2
    output=str(exit_codes[1][0].decode("utf-8"))
    
    if output:
        print("executou com SUCESSO!!!")
        print(output)
    else:
        err=str(exit_codes[1][1].decode("utf-8"))
        print("executou com ERRO!!!")
        print(err)

    ## OUTPUT 3        
    output=str(exit_codes[2][0].decode("utf-8"))

    if output:
        print("executou com SUCESSO!!!")
        print(output)
    else:
        err=str(exit_codes[2][1].decode("utf-8"))
        print("executou com ERRO!!!")
        print(err)
                        
    print(datetime.datetime.now())
    
