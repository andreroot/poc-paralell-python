

import datetime
from datetime import date, timedelta

# import asyncio
# import concurrent.futures
import pandas as pd


from conectores.conectar_sql_server import ConectSqlServer

from resourcex.utilitarios import Utils

#iniciar implementacao do envio kinesis - 06/10
#
#importar biblioteca importada pelo github: github.com/grupo-safira/geralog-monitoria.git
#
from geralog.decorator_log.monitoria import *
# from geralog.decorator_log.put import stream_log_method_decorator

# def conn():
#     conn = ConectSqlServer()

#     self.get_engine_pyodbc = conn.get_engine_pyodbc()
#     self.get_engine_sqlalchemy = conn.get_engine_sqlalchemy()

class ExecProcessSqlServer:
    
    def __init__(self):
        pass

    def get_data(self, sql_alert):

        conn = ConectSqlServer()

        get_engine_sqlalchemy = conn.get_engine_sqlalchemy()
        
        df = pd.DataFrame()
        df = pd.read_sql_query(sql_alert, get_engine_sqlalchemy)
        
        start_date = df['start_date'].drop_duplicates().values.tolist()
        str_start_date = start_date[0]
        
        # date(start_date.year, start_date.month, start_date.day).strftime('%Y-%m-%dT23:59:59.000Z')
        
        start_date = datetime.datetime.strftime(str_start_date, '%Y-%m-%dT23:59:59.000Z')
                
        return start_date
            

    @stream_log_method_decorator('sql-server/InformacaoComercial')        
    def InformacaoComercial(self, start_date, curva):

        conn = ConectSqlServer()

        get_engine_pyodbc = conn.get_engine_pyodbc()
        ## Querys para Selecionar o Portfólio Histórico

        start_date = 'NULL' if start_date is None else f"'{start_date}'"
        
        script_proc =(f"""                      
                    BEGIN

                        SET NOCOUNT ON;
                        SET ANSI_WARNINGS OFF;
                        
                        exec [Book].[InformacaoComercial] {start_date}, '{curva}';
                                                
                        END
                            
                        """)
        print(script_proc)
        crsr = get_engine_pyodbc.execute(script_proc)
        crsr.commit()
        crsr.close()
        
        msg = "execução [Book].[InformacaoComercial] ok"
        
        return msg

    @stream_log_method_decorator('sql-server/STP_HistoricoResultado_log')   
    def STP_HistoricoResultado_log(self, start_date,end_date, curva):

        conn = ConectSqlServer()

        get_engine_pyodbc = conn.get_engine_pyodbc()
        
        ## Querys para Selecionar o Portfólio Histórico
        script_proc =(f""" 
                        BEGIN

                        SET NOCOUNT ON;
                        SET ANSI_WARNINGS OFF;
                        
                            --DECLARE @datamax as Datetime
                            --DECLARE @datahoje as Datetime
                            
                            --SET @datahoje = GETDATE()
                            --SET @datamax = (SELECT MAX(Datahistorico) 
                            --               FROM Book.[Book].[HistoricoResultado_log])
                            --SET @datahoje = GETDATE()
                                            
                            EXEC BOOK.[STP_HistoricoResultado_log] '{start_date}', '{end_date}', '{curva}' ;
                            
                            END
                            
                            """)

        crsr = get_engine_pyodbc.execute(script_proc)
        crsr.commit()
        crsr.close()
        
        msg = "execução BOOK.STP_HistoricoResultado_log ok"
        
        return msg
    
    @stream_log_method_decorator('fluxo_caixa_historico_api_sgr_historico_credito')    
    def sgr_historico_credito(self, data_historico=str(date.today()), show_info=False):
        try:
            
            pload = {"date": str(data_historico)}
            response = requests.post('http://172.16.10.5:3014/credito-dashboard/saveDashboard',data = pload)
            
            str_warning = f"""{response.text}
                                {response.url}
                                {response.status_code}
                                {response}
                                {response.headers}"""
            
            if response.status_code == 201:
                print(f'Salvo Histórico Crédito - SGR para {data_historico}')
                if show_info:
                    print(str_warning)
            else:
                raise Warning(f'Status diferente de 201:\n{str_warning}')
        
            msg = "execução api sgr_historico_credito ok"

        except requests.exceptions.RequestException as error:  # This is the correct syntax
            print(f'Erro: {error} - {response.text}')
            print(response.status_code)
            
            msg = "execução api sgr_historico_credito nok"

        return msg

    # def Loop_sgr_historico_credito(datainicio=str(date.today()), datafim=str(date.today()), show_info=False):
    #     data_ini = datainicio
    #     data_fim = datafim
    #     if data_ini > data_fim:
    #         raise ValueError('Data de ínicio maior que a data final')
    #     while data_ini <= data_fim:
    #         print(data_ini)
    #         sgr_historico_credito(data_historico=data_ini, show_info=show_info)
    #         data_ini = data_ini + timedelta(days=1)

    def replica_operation_history(self, start_date):


        conn = ConectSqlServer()

        get_engine_pyodbc = conn.get_engine_pyodbc()
        ## Querys para Selecionar o Portfólio Histórico

        start_date = 'NULL' if start_date is None else f"'{start_date}'"
        
        script_proc =(f"""                      
                    BEGIN

                        SET NOCOUNT ON;
                        SET ANSI_WARNINGS OFF;
                        
                        EXEC Modelo.dbo.STP_operation_history;
                                                
                        END
                            
                        """)
        
        crsr = get_engine_pyodbc.execute(script_proc)
        crsr.commit()
        crsr.close()
        
        msg = "execução dbo.STP_operation_history ok"
        
        return msg



    def comparacao_poc_historico(self, data_d0: str = None, data_d1: str = None):

        conn = ConectSqlServer()

        get_engine_pyodbc = conn.get_engine_pyodbc()
        
        ## Querys para Selecionar o Portfólio Histórico
        ## Querys para Selecionar o Portfólio Histórico
        if data_d0 is None or data_d1 is None:
            data_d0 = "(select max(data) from book.curva.Curva_Fwd where curva = 'Oficial')"
            data_d1 = "(select max(data) from book.curva.Curva_Fwd where curva = 'Oficial' and data < @data_curva_d0)"
        else:
            data_d0 = f"'{data_d0}'"
            data_d1 = f"'{data_d1}'"
        
        script_proc =(f"""
                BEGIN

                SET NOCOUNT ON;
                SET ANSI_WARNINGS OFF;
                        
                DECLARE @data_curva_d0 AS DATE = {data_d0}
                DECLARE @data_curva_d1 AS DATE = {data_d1}
                DECLARE @vpl AS NVARCHAR(255) = (
                    select top 1 vpl
                    from treinamento.gpires.tabelavpl
                    where dateInsert = (
                        SELECT MAX(dateInsert) FROM treinamento.gpires.tabelavpl
                    )
                );

                EXEC Modelo.dbo.STP_gera_bases_POC_Historico @vpl, @data_curva_d0, @data_curva_d1, 'Oficial', 'Oficial', NULL;
                
                END
                
                """)

        crsr = get_engine_pyodbc.execute(script_proc)
        crsr.commit()
        crsr.close()
        
        msg = "execução dbo.STP_gera_bases_POC_Historico ok"
        
        return msg




    # @stream_log_method_decorator('sql-server/exec_dbo_teste')        
    # def exec_dbo_teste(self, data):

    #     conn = ConectSqlServer()

    #     get_engine_pyodbc = conn.get_engine_pyodbc()
    #     #get_engine_sqlalchemy = conn.get_engine_sqlalchemy()
            
    #     # Converte a Data para o formato do EXEC Histórico
    #     # data = date(data.year, data.month, data.day).strftime('%Y-%m-%dT23:59:59.000Z')

    #     script_proc = f"""
    #                     BEGIN

    #                     SET NOCOUNT ON;
    #                     SET ANSI_WARNINGS OFF;
                        
    #                     DECLARE @date_insert_1 		varchar(100) ;
    #                     SET @date_insert_1 = '{data}';
                        
    #                     EXEC dbo.teste @date_insert_1 ;
                        
    #                     END
                        
    #                     """
                                
        
    #     crsr = get_engine_pyodbc.execute(script_proc)
    #     crsr.commit()
    #     crsr.close()
        
    #     print(script_proc)
        
    #     msg = "execução dbo.teste ok"
        
    #     return msg

    # # def Loop_exec_dbo_teste(start_date,end_date):
    # #     start_date = start_date
    # #     end_date  = end_date
        
    # #     for data in rrule(DAILY, dtstart=start_date, until=end_date):
    # #         print(data.strftime('%Y-%m-%d'))
    # #         exec_dbo_teste(data)
            
    # #     return


    # @stream_log_method_decorator('sql-server/STP_HistoricoPosicaoContraparte_log')    
    # def STP_HistoricoPosicaoContraparte_log(self, start_date):


    #     conn = ConectSqlServer()

    #     get_engine_pyodbc = conn.get_engine_pyodbc()
    #     ## Querys para Selecionar o Portfólio Histórico

    #     start_date = 'NULL' if start_date is None else f"'{start_date}'"
        
    #     script_proc =(f"""                      
    #                 BEGIN

    #                     SET NOCOUNT ON;
    #                     SET ANSI_WARNINGS OFF;
                        
    #                     exec BOOK.[STP_HistoricoPosicaoContraparte_log] {start_date};
                                                
    #                     END
                            
    #                     """)
        
    #     crsr = get_engine_pyodbc.execute(script_proc)
    #     crsr.commit()
    #     crsr.close()
        
    #     msg = "execução [Book].[STP_HistoricoPosicaoContraparte_log] ok"
        
    #     return msg

    