import requests
import json
import os
from datetime import  datetime


from conectores.conectar_aws import ConectAWS
from botocore.exceptions import ClientError

#iniciar implementacao do envio kinesis - 06/10
#
#importar biblioteca importada pelo github: github.com/grupo-safira/geralog-monitoria.git
#
from geralog.decorator_log.monitoria import *

#buscar credencial que esta gravda dentro secret do aws
def get_secret_sql(secret_name):

    cred = ConectAWS() 
    awssecret = cred.get_svc_user_credentials()

    #secret_name =  "nome da secret"
    # api-thunders/prod/gruposafira
    # api-thunders/prod/safiravarejo
    region = os.getenv("AWS_DEFAULT_REGION")
   
    client = awssecret.client(
        service_name='secretsmanager',
        region_name=region 
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']

    # Your code goes here.

    return json.loads(secret)


class ConsultaThundersApi:

    def __init__(self):

        #print('buscar credencial na aws')
        
        self.adm_user_password = get_secret_sql('api-thunders/prod/gruposafira')

        self.varejo_user_password =  get_secret_sql('api-thunders/prod/safiravarejo')

        # self.payload = json.dumps({
        #     "email": self.ehub_email, #os.getenv("EHUB_EMAIL"),
        #     "password": self.ehub_password, #os.getenv("EHUB_PASSWORD"),
        #     "companyExternalCode": self.ehub_company_code, #os.getenv("EHUB_COMPANY_CODE")
        # })

        # self.headers = {
        #     'apikey': self.ehub_api_key,  #os.getenv("EHUB_API_KEY"),
        #     'Content-Type': 'application/json',
        #     'Cookie': 'cookiesession1=678A3E63MNOQRSTUVWXYZABCDEFGC151'
        # }


    def get_token(self, banco):

        # print('Getting access token...')
        
        try:

            url = "https://api.novo.thunders.com.br/connect/token"

            #--> Thunders: Comercial
            if banco == 'BookComercial':
                credentials = self.varejo_user_password

            #--> Thunders: Safira
            elif banco == 'Book':
                credentials = self.adm_user_password

            response = requests.request("POST", url, data = credentials)

            #print(response.text.encode('utf8'))

            token = response.json().get('access_token')

        except requests.exceptions.RequestException as error:  # This is the correct syntax
            
            token = None

            if banco == 'BookComercial':
                erro = self.tratativa_erro_api("get_token", error)

            elif banco == 'Book':
                erro = self.tratativa_erro_api("get_token", error)

            print(f'TOKEN | ERROR API:{erro}')

        return token

        
    # def cenarios_ativos(self, access_token): 

    #     match_endpoint = f"https://api.novo.thunders.com.br/gw/marketdata/price-curves/scenarios" 
    #     payload = ""
    #     headers = {'authorization': 'Bearer '+access_token, 'content-type': 'application/json'}
        
    #     print("Conectando na api thunders - validar inserção de novos dados")

    #     try:
    #         match_endpoint = requests.get(match_endpoint, data=payload, headers=headers)

    #         try:
    #             matching_data = match_endpoint.json()

    #         except requests.exceptions.JSONDecodeError as err:
    #             print ("Erro no retorno do json(verificar retorno da api):", err)
    #             matching_data = {'msg_json': err, 'msg_hhtp':''}

    #         match_endpoint.raise_for_status()
         
    #         if str(match_endpoint.status_code)=='200':
    #             print(f"Retorno ok: {match_endpoint.status_code} | inserção com sucesso")

    #     except requests.exceptions.HTTPError as errh:
    #         print ("Erro no retorno da api(verificar token):", errh, str(match_endpoint.status_code))
    #         matching_data['msg_hhtp']=errh
        
    #     return matching_data

    # def cenario_info(self, scenarioId, year, access_token):

    #     match_endpoint = f"https://api.novo.thunders.com.br/gw/marketdata/price-curves/scenarios/details?scenarioId={scenarioId}&Year={year}" 
    #     payload = ""
    #     headers = {'authorization': 'Bearer '+access_token, 'content-type': 'application/json'}
        
    #     print(f"Conectando na api thunders - validar scenario {scenarioId} ")
        
    #     try:
    #         match_endpoint = requests.get(match_endpoint, data=payload, headers=headers)

    #         try:
    #             matching_data = match_endpoint.json()

    #         except requests.exceptions.JSONDecodeError as err:
    #             print ("Erro no retorno do json(verificar parametro da api):", err)
    #             matching_data = {'msg_json': err, 'msg_hhtp':''}

    #         match_endpoint.raise_for_status()
         
    #         if str(match_endpoint.status_code)=='200':
    #             print(f"Retorno ok: {match_endpoint.status_code} | inserção de scenario {scenarioId} com sucesso")

    #     except requests.exceptions.HTTPError as errh:
    #         print ("Erro no retorno da api(verificar token):", errh, str(match_endpoint.status_code))
    #         matching_data['msg_hhtp']=errh
        
    #     return matching_data
    
    @stream_log_method_decorator_error('dados/api-thunders/error_token_thunders')
    def tratativa_erro_api(self, msg, error):
        
        descr_status_decorator = error
        
        return descr_status_decorator