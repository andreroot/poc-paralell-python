import pandas as pd
import requests
import json
from pandas import json_normalize
import os
from datetime import date, datetime, timedelta


from typing import Callable
from conectores.conectar_aws import ConectAWS
from botocore.exceptions import ClientError

#buscar credencial que esta gravda dentro secret do aws
def get_secret_sql():

    cred = ConectAWS() 
    awssecret = cred.get_svc_user_credentials()

    secret_name = "acesso-ehub" 
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


class ConsultaEHUBApi:

    def __init__(self):

        print('buscar credencial na aws')
        xpass = get_secret_sql()

        self.ehub_email = xpass['email']
        self.ehub_password =  xpass['password']
        self.ehub_company_code =  xpass['companyExternalCode']
        self.ehub_api_key =  xpass['apikey']

        self.payload = json.dumps({
            "email": self.ehub_email, #os.getenv("EHUB_EMAIL"),
            "password": self.ehub_password, #os.getenv("EHUB_PASSWORD"),
            "companyExternalCode": self.ehub_company_code, #os.getenv("EHUB_COMPANY_CODE")
        })

        self.headers = {
            'apikey': self.ehub_api_key,  #os.getenv("EHUB_API_KEY"),
            'Content-Type': 'application/json',
            'Cookie': 'cookiesession1=678A3E63MNOQRSTUVWXYZABCDEFGC151'
        }


    def get_token(self):

        try:
            erro = None

            print('TOKEN | CONSULTAR API LOGIN')
            

            url = "https://api-ehub.bbce.com.br/bus/v2/login"

            payload = self.payload
            headers = self.headers

            response = requests.request("POST", url, headers=headers, data = payload, timeout=5)

            #print(response.text.encode('utf8'))

            token = response.json().get('idToken')

        except requests.exceptions.RequestException as error:  # This is the correct syntax
            
            token = None
            #erro = self.tratativa_erro_api("api login", error)
            print(f'TOKEN | ERROR API:{error}')

        return token
    
    # def get_api_negocios(self, date_ini, x_token):

    #     try:
    #         erro = None
    #         print(f'NEGOCIO | CONSULTAR API - DADOS DO DIA:{date_ini}')
            
    #         url = 'https://api-ehub.bbce.com.br/bus/v1/all-deals/report'

    #         params = {
    #             'initialPeriod': date_ini #'2023-11-20'
    #             #'initialPeriod': date.today().strftime("%Y-%m-%d")
    #         }
            
            
    #         self.headers.update({'Authorization':'Bearer '+ x_token})
            
    #         response = requests.get(url, headers=self.headers, params = params) #, timeout=5
    #         #response = requests.get(url, headers=headers)
            
    #         df = response.json()
    #         df = pd.json_normalize(df)

    #         if 'tendency' in df.columns :
    #             print('tendency column is present')
    #         else:
    #             print('tendencycolumn is not present')
    #             df['tendency'] = None
        
    #     except requests.exceptions.RequestException as error:  # This is the correct syntax
    #         df = pd.DataFrame()
    #         #erro = self.tratativa_erro_api("api negociação", error)
    #         print(f'NEGOCIO | ERROR API: {error}')

    #     return df

    # def get_api_fwd_curve(self, days, x_token):

    #     try:

    #         erro = None

    #         dateList = pd.date_range(date.today() - timedelta(days=days), date.today()).strftime("%Y-%m-%d").tolist()
            
    #         df = pd.DataFrame()
            
    #         for dia in dateList:
                
    #             print(f'CURVE-BBCE-FWD | CONSULTAR API CURVA DIARIA: {dia}')
                
    #             url = 'https://api-ehub.bbce.com.br/bus/v1/curve/bbce-fwd'
                
    #             params_con = {
    #                 'referenceDate': dia
    #             }
                
    #             params_i5 = {
    #                 'referenceDate': dia,
    #                 'energyType': 'I5'
    #             }
                
    #             self.headers.update({'Authorization':'Bearer '+ x_token})

    #             # response = requests.get(url, headers=self.headers)

    #             # curves = response.json()

    #             response_con = requests.get(url, headers=self.headers, params=params_con) # , timeout=5
                
    #             response_i5 = requests.get(url, headers=self.headers, params=params_i5) # , timeout=5
                
    #             #print(response.text.encode('utf8'))
                
    #             df_con = response_con.json()
    #             df_con =  pd.DataFrame.from_dict(df_con)
                
    #             df_i5 = response_i5.json()
    #             df_i5 =  pd.DataFrame.from_dict(df_i5)

    #             print(dia + ': CON - ' + str(df_con.shape[0]) + ' - I5 - ' + str(df_i5.shape[0]))
    #             df = pd.concat([df, df_con, df_i5]).reset_index(drop=True)

    #     except requests.exceptions.RequestException as error:  # This is the correct syntax

    #         df = pd.DataFrame()
    #         #erro = self.tratativa_erro_api("api curve-bbce-fwd", error)
    #         print(f'CURVE-BBCE-FWD | ERROR API: {error}')

    #     return df
            
    # def get_api_fwd_curve_call(self, x_token):

    #     try:
        
    #         #token = get_access_token()
    #         erro = None

    #         print('CURVE-CALL | CONSULTA API CURVA ALL ')
            
    #         url = 'https://api-ehub.bbce.com.br/bus/v1/curve/call'
            
    #         self.headers.update({'Authorization':'Bearer '+ x_token})
            

    #         response_con = requests.get(url, headers=self.headers)
            
    #         df = response_con.json()
    #         df =  pd.DataFrame.from_dict(df)
        
    #     except requests.exceptions.RequestException as error:  # This is the correct syntax

    #         df = pd.DataFrame()
    #         #erro = self.tratativa_erro_api("api curve-call", error)
    #         print(f'CURVE-CALL | ERROR API: {error}')

    #     return df

    # def get_api_ticker_id(self, id, x_token):

    #     try:

    #         erro = None
        
    #         print('TICKERS | CONSULTAR API PRODUTO PELO ID: {}'.format(id))
            
    #         url = 'https://api-ehub.bbce.com.br/bus/v2/tickers/{}'.format(int(id))
            
    #         self.headers.update({'Authorization':'Bearer '+ x_token})
            
    #         params = {
    #             'walletId': '941'
    #         }
            
    #         response = requests.get(url, headers=self.headers, params = params)
            
    #         df = response.json()
    #         #print(df)

    #         df = json.dumps(df)
    #         #print(df)

    #         df = json.loads(df)
    #         #print(df)

    #         df= json_normalize(df)
    #         #print(df)

    #     except requests.exceptions.RequestException as error:  # This is the correct syntax

    #         df = pd.DataFrame()
    #         #erro = self.tratativa_erro_api("api tickers", error)
    #         print(f'TICKERS | ERROR API: {error}')

    #     return df

    # def get_api_produtos(self, x_token):

    #     try:

    #         erro = None
        
    #         print('TICKERS | CONSULTAR API PRODUTOS EM GERAL')
            
    #         url = 'https://api-ehub.bbce.com.br/bus/v2/tickers'
            
    #         self.headers.update({'Authorization':'Bearer '+ x_token})
            
    #         response = requests.get(url, headers=self.headers)
            
    #         df = response.json()
    #         df = json_normalize(df)
            
    #         print('Produtos EHUB: ' + str(df.shape))
            
    #         print('Produtos extraidos com sucesso!')
        
    #     except requests.exceptions.RequestException as error:  # This is the correct syntax

    #         df = pd.DataFrame()
    #         #erro = self.tratativa_erro_api("api tickers", error)
    #         print(f'TICKERS | ERROR API: {error}')

    #     return  df

    # def get_api_links(self,x_token):
        
    #     try:

    #         erro = None
        
    #         print('LINK | CONSULTAR API LINK')
            
    #         url = 'https://api-ehub.bbce.com.br/bus/v1/link'
            
    #         self.headers.update({'Authorization':'Bearer '+ x_token})
            
    #         response = requests.get(url, headers=self.headers)

    #         df = response.json()
    #         df = json.dumps(df)
    #         df = json.loads(df)
    #         df = json_normalize(df)
        
    #     except requests.exceptions.RequestException as error:  # This is the correct syntax

    #         df = pd.DataFrame()
    #         #erro = self.tratativa_erro_api("api link", error)
    #         print(f'LINK | ERROR API: {error}')

    #     return df
