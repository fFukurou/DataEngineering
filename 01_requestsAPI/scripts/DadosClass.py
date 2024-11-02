import requests
import pandas as pd
from personal_info import access_token


class DadosRepositorios:

    def __init__(self, owner):
        self.owner = owner
        self.api_base_url = 'https://api.github.com'
        self.access_token = access_token
        self.headers = {'X-GitHub-Api-Version': '2022-11-28',
                        "Authorization": f"token {access_token}",
                        "Accept": "application/vnd.github.v3+json"}

    def lista_repositorios(self):
        repos_list = []
        page_num = 1

        while True:
            try:
                url_page = f'{self.api_base_url}/users/{self.owner}/repos?per_page=100&page={page_num}'
                response = requests.get(url_page, headers=self.headers)
                if len(response.json()) == 0:
                    break
                repos_list.append(response.json())
                page_num += 1
            except:
                repos_list.append(None)
                break
        return repos_list

    def nomes_repos(self, repos_list):
        repo_names = []
        for page in repos_list:
            for repo in page:
                try:
                    repo_names.append(repo['name'])

                except:
                    pass

        return repo_names

    def nomes_linguagens(self, repos_list):
        repo_languages = []
        for page in repos_list:
            for repo in page:
                try:
                    repo_languages.append(repo['language'])
                except:
                    pass

        return repo_languages

    def cria_df_linguagems(self):
        repositorios = self.lista_repositorios()
        nomes = self.nomes_repos(repositorios)
        linguagens = self.nomes_linguagens(repositorios)
        
        dados = pd.DataFrame()
        dados['repository_name'] = nomes
        dados['repository_language'] = linguagens
        
        return dados

