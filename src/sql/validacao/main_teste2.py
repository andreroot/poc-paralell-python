import datetime

from old.process_sql_old import ExecProcessSqlServer
from resourcex.utilitarios import Utils

if __name__=='__main__':
    exec = ExecProcessSqlServer()
    
    print(datetime.datetime.now())

    start_date = exec.get_data("(select DATEADD( DAY, -1, max(data) ) start_date from book.curva.Curva_Fwd where curva = 'Oficial')")

    util = Utils()
    util.create_file("teste2.txt",start_date)
    
    print(start_date)

    print(datetime.datetime.now())
    