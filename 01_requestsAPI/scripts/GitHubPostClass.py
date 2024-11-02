import requests
import base64
from personal_info import access_token
import json

class ManipulaRepositorios:
    def __init__(self, username):
        self.username = username
        self.api_base_url = 'https://api.github.com'
        self.access_token = access_token
        self.headers = {'X-GitHub-Api-Version': '2022-11-28',
                "Authorization": f"token {access_token}",
                "Accept": "application/vnd.github.v3+json"}
        
        
    def cria_repo(self, nome_repo):
        data = {
            'name': nome_repo,
            'description': 'Dados de repos de algumas empresas',
            'private': False
        }
        
        response = requests.post(f"{self.api_base_url}/user/repos", data=json.dumps(data), headers=self.headers)
        print(f"status_code criação do repositório: {response.status_code}")
        
        
    def add_arquivo(self, nome_repo, nome_arquivo, caminho_arquivo):
        # Codificando o arquivo
        with open(caminho_arquivo, 'rb') as file:
            file_content = file.read()
        encoded_content = base64.b64encode(file_content)
        
        # Realizando o upload
        url = f"{self.api_base_url}/repos/{self.username}/{nome_repo}/contents/{nome_arquivo}"
        data = {
        'message': 'added new file',
        'content': encoded_content.decode('utf-8'),
        }
        
        response = requests.put(url, json=data, headers=self.headers)
        print(f'status_code do upload do arquivo: {response.status_code}')
        


        
        