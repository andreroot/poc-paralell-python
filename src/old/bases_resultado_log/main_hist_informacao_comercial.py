
import datetime

from old.process_sql import ExecProcessSqlServerNew

if __name__=='__main__':
        
    print(datetime.datetime.now())

    execx = ExecProcessSqlServerNew()

    # Historico Posição Contraparte
    execx.exec_informacao_comercial('Book','proc_book_informacao_comercial.sql')  
            
    print(datetime.datetime.now())
