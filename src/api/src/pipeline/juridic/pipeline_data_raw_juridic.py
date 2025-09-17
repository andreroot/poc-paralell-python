from pipeline.get_data_api import GetDataApi
from pipeline.processing_data_paralell_v1 import GetApi, ExecuteParalell
from conectores.conectar_sql_server import ConectSqlServer

import pandas as pd

class GetDataJuridic:
    
    def __init__(self, conn: ConectSqlServer):
        self.conn = conn

    def get_list_urls(self, Thunders, p_url, p_uid):
        """    Gera uma lista de URLs para requisições de workflowItemid e contractCode.
        FULL
        Args:
            Thunders: Conexão com o banco de dados. 
            p_url: URL base para as requisições.
            p_uid: Sufixo a ser adicionado à URL.
        Returns:
            list_urls_ids: Lista de tuplas contendo URLs, workflowItemid e contractCode.
        """

        # Create a SQLAlchemy engine
        engine = self.conn.get_engine_sqlalchemy(Thunders)
                
        workflowitem_ids = pd.read_sql(
                """
            select workflowItemid, contractCode
            from apiworkflows.juridicUnion
            where workflowitemid is not null
        """,
                engine,
            )

                
        # A lista contém "workflowItemid" e "contracCode"
        list_workflowitem_ids = workflowitem_ids.apply(lambda x: 
            {
                "workflowItemid": x["workflowItemid"],
                "contractCode": x["contractCode"]
            }, axis=1).to_list()

        # Gerar a lista de URLs
        list_urls_ids = []
        # Montar a lista de URLs com workflowItemid e contracCode
        list_urls_ids=[(f'{p_url}{item["workflowItemid"]}{p_uid}', item["workflowItemid"], item["contractCode"])for  item in list_workflowitem_ids]
      
        print(f"Fazendo requisição de comentários dos workflowItemid ({len(workflowitem_ids)} workflowItemid)")
        # # print(workflow_item_id, contract_code)
        # url = f"{p_url}{workflow_item_id}{p_uid}"
        return list_urls_ids


    def get_list_urls_incremental(self, Thunders, p_url, p_uid):
        """    Gera uma lista de URLs para requisições de workflowItemid e contractCode.
        INCREMENTAL somente apartir do que foi criado de 2025-01-01 para frente.
        Args:
            Thunders: Conexão com o banco de dados. 
            p_url: URL base para as requisições.
            p_uid: Sufixo a ser adicionado à URL.
        Returns:
            list_urls_ids: Lista de tuplas contendo URLs, workflowItemid e contractCode.
        """
        # Create a SQLAlchemy engine
        engine = self.conn.get_engine_sqlalchemy(Thunders)
                
        # workflowitem_ids = pd.read_sql(
        #         """
        #     select workflowItemid, contractCode
        #     from apiworkflows.juridicUnion
        #     where workflowitemid is not null
        #     and createdDate >= dateadd(day, -7, getdate())
        # """,
        #         engine,
        #     )

        workflowitem_ids = pd.read_sql(
                """
            select workflowItemid, contractCode
            from apiworkflows.juridicUnion
            where createdDate >= '2025-01-01'
        """,
                engine,
            )
        
        # A lista contém "workflowItemid" e "contracCode"
        list_workflowitem_ids = workflowitem_ids.apply(lambda x: 
            {
                "workflowItemid": x["workflowItemid"],
                "contractCode": x["contractCode"]
            }, axis=1).to_list()

        # Gerar a lista de URLs
        list_urls_ids = []
        # Montar a lista de URLs com workflowItemid e contracCode
        list_urls_ids=[(f'{p_url}{item["workflowItemid"]}{p_uid}', item["workflowItemid"], item["contractCode"])for  item in list_workflowitem_ids]
              
        print(f"Fazendo requisição de comentários dos workflowItemid ({len(workflowitem_ids)} workflowItemid)")

        return list_urls_ids
            
    def run_getapi_juridic_workflowitem(self, Thunders, extract_full=True):
        """    Obtém dados de workflowItemid e contractCode da API Juridic.
        Args:

            Thunders: Conexão com o banco de dados.
            extract_full: Booleano para indicar se a extração é completa ou incremental.
        Returns:
            result_concat: DataFrame concatenado com os dados obtidos da API.
        """
        # EXEMPLO DE URL    
        # https://api.novo.thunders.com.br/gw/legal/api/workflowItem/019b1cc4-3609-43f3-90c3-c1032da1bb6d/lifecycle
        # https://api.novo.thunders.com.br/gw/legal/api/workflowItem/019b1cc4-3609-43f3-90c3-c1032da1bb6d   

        # 5b25b4f4-dec2-4b93-8849-5a7303dc692c	
        url = f"https://api.novo.thunders.com.br/gw/legal/api/workflowItem/"
        
        if extract_full:
            # url = f"https://api.novo.thunders.com.br/gw/legal/api/workflowItem/019b1cc4-3609-43f3-90c3-c1032da1bb6d/lifecycle"
            # url = f"https://api.novo.thunders.com.br/gw/legal/api/workflowItem/5b25b4f4-dec2-4b93-8849-5a7303dc692c/lifecycle"
            # url = f"https://api.novo.thunders.com.br/gw/legal/api/workflowItem/5b25b4f4-dec2-4b93-8849-5a7303dc692c/lifecycle"
            generate_urls = self.get_list_urls(Thunders, url, "/lifecycle")
        else:
            generate_urls = self.get_list_urls_incremental(Thunders, url, "/lifecycle")

        # # GERAR A LISTAGEM DE URLS
        # getapi = GetApi()
        # #generate_urls = [('https://api.novo.thunders.com.br/gw/legal/api/workflowItem/019b1cc4-3609-43f3-90c3-c1032da1bb6d/lifecycle', '019b1cc4-3609-43f3-90c3-c1032da1bb6d')]
        # generate_urls = getapi.generate_list_urls_juridic(url, list_urls, "/lifecycle")

        # ENVIAR LISTAGEM DE URLS PARA OBTER RETORNO EM JSON 
        exec = ExecuteParalell()
        result_concat = exec.execute_processo_paralell_juridic(Thunders, generate_urls, 'stepDetails')
        
        # result_concat.reset_index(drop=True, inplace=True)
        # # print("Data fetch and processing completed.")
        return result_concat