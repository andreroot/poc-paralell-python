#!/usr/bin/python
# -*- coding: utf-8 -*-
import codecs

import re
import os

class Utils:

    def __init__(self):
        self.path_src = os.getcwd()

    def read_file(self, file):
        
        file = f'/home/node/github/processos_sql_server/src/v1/sql/{file}'

        f=codecs.open(file, "r", 'utf-8')
        
        conteudo=f.readlines()
        
        strsql = ""

        for ln in conteudo:
            strsql += ln

        # print(f"sql: {strsql}")
        return strsql
                
    def read_file_win(self, file):
        
        
        file = f'C:\\Users\\abarbosa\\Documents\\projetos\\processos_sql_server\\src\\v1\\sql\\{file}'

        f=codecs.open(file, "r", 'utf-8')
        conteudo=f.readlines()
        strsql = ""
    
        for ln in conteudo:
            strsql += ln

        return strsql
