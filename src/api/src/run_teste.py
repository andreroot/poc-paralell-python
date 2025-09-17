from geralog.decorator_log.monitoria import *
from resources.send_webhook import webhook

def execute_error(e):
    return e


def main():

    webhook(f'teste', "teste")

    decorator_monitoria = stream_log_method_decorator_error(f'teste')(execute_error)
    decorator_monitoria("teste")
    
    msg="teste"
    return msg

if __name__=="__main__":
    
    main()