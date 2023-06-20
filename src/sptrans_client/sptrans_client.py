import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

class SPTransClient():

    def __init__(self):

        self.session = requests.Session()
        self.url = 'http://api.olhovivo.sptrans.com.br/v2.1/'
        self.TOKEN_API_OLHOVIVO = os.getenv('TOKEN_API_OLHOVIVO')

    
    def auth(self) -> str:

     '''
        This function is responsible for authenticating our application with SPTRANS using a user token.
        The function should return a [bool], with TRUE for authenticated or FALSE for not authenticated.
     
        :return: A variable that receives two string with the status text (true or false) and status code (200, or 400 or 500, etc...).
        :rtype: string
     '''

     endpoint = f'Login/Autenticar?token={self.TOKEN_API_OLHOVIVO}'
     response = self.session.post(self.url + endpoint)

     return response.text, response.status_code
    

    def _get(self, endpoint : str) -> json:

        '''
            This function is responsible for retrieving data from the Olho Vivo API by providing the API URL along with the route (endpoint).
            The function should return a [Json] object with the content of the requested endpoint.

            :param endpoint: The parameter that contains the endpoint to be concatenated with the API URL, for example (Login/Autenticar?token={TOKEN_API_OLHOVIVO} or /Posicao)
            :type endpoint: str

            :return: Variable that contains data in JSON format.
            :rtype: json
        '''

        response = self.session.get(self.url + endpoint)
        
        return response.json()
        

    def bus_position(self) -> json:

        '''
            :param hr: Horário de referência da geração das informações.
            :type hr: str

            [{}]l Relação de linhas localizadas onde:

                :param c: Letreiro completo.
                :type c: string
                :param cl: Código identificador da linha.
                :type cl: string
                :param sl: Sentido de operação onde 1 significa de Terminal Principal para Terminal Secundário e 2 de Terminal Secundário para Terminal Principal.
                :type sl: int
                :param lt0: Letreiro de destino da linha.
                :type lt0: string
                :param lt1: Letreiro de origem da linha.
                :type lt1: string
                :param qv: Quantidade de veículos localizados.
                :type qv: int

            [{}]vs Relação de veículos localizados, onde: 

                :param p: Prefixo do veículo.
                :type p: int
                :param a: Indica se o veículo é (true) ou não (false) acessível para pessoas com deficiência.
                :type a: bool
                :param ta: Indica o horário universal (UTC) em que a localização foi capturada. Essa informação está no padrão ISO 8601.
                :type ta: string
                :param py: Informação de latitude da localização do veículo.
                :type py: double
                :param px: Informação de longitude da localização do veículo.
                :type px: double
            
            :return: A json that contains characteristic data of buses and their geolocational positions.
            :rtype: json
        '''
        endpoint = 'Posicao'
        return self._get(endpoint)