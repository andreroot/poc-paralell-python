
import datetime

from old.process_sql import ExecProcessSqlServerNew

if __name__=='__main__':
        
    print(datetime.datetime.now())

    execx = ExecProcessSqlServerNew()

    # Historico Posição Contraparte
    execx.exec_historico_resultado_log('Book','proc_book_HistoricoResultado_log.sql')  
            
    print(datetime.datetime.now())
