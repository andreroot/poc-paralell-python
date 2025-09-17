import re
import os

class Utils:

    def __init__(self):
        self.path_src = os.getcwd()
            
    def read_file_sql(self, file):

        
        if re.findall(r'administrador',self.path_src):
            file = f'/home/administrador/projetos/dados-poc-async-paralell/{file}'
        else:
            file = f'/home/andre/projetogithub/dados-poc-async-paralell/{file}'

        f=open(file.encode('utf-8'), "r")
        conteudo=f.readlines()
        strsql = ""

        for ln in conteudo:
            strsql += ln

        # if (len(parametro)>0 and parametro!=None):
        #     strsql=strsql.format(parametro=parametro)
        return strsql