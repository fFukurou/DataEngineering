import json
import csv

from processamento_dados import Dados

# def leitura_json(path_json):
#     with open(path_json, 'r') as file:
#         dados_json = json.load(file)
#         return dados_json


# def leitura_csv(path_csv):
#     dados_csv = []

#     with open(path_csv, 'r') as file:
#         spamreader = csv.DictReader(file, delimiter=',')

#         for row in spamreader:
#             dados_csv.append(row)

#     return dados_csv


# def leitura_dados(path):
#     dados = []
#     tipo_arquivo = path.split('.')[1]

#     if tipo_arquivo == 'csv':
#         dados = leitura_csv(path)

#     elif tipo_arquivo == 'json':
#         dados = leitura_json(path)

#     return dados

# def get_columns(dados):
#     return list(dados[-1].keys())


# def rename_columns(dados, key_mapping):
#     new_dados_csv = []

#     for old_dict in dados:
#         dict_temp = {}
#         for old_key, value in old_dict.items():
#             dict_temp[key_mapping[old_key]] = value

#         new_dados_csv.append(dict_temp)
#     return new_dados_csv


# def size_data(dados):
#     return len(dados)


# def join(dadosA, dadosB):
#     combined_list = []
#     combined_list.extend(dadosA)
#     combined_list.extend(dadosB)
#     return combined_list


# def transformando_dados_tabela(dados, nomes_colunas):
#     dados_combinados_tabela = [nomes_colunas]

#     for row in dados:
#         linha = []
#         for coluna in nomes_colunas:
#             linha.append(row.get(coluna, 'Indisponivel'))

#         dados_combinados_tabela.append(linha)

#     return dados_combinados_tabela


# def salvando_dados(dados, path):
#     with open(path, 'w') as file:
#         writer = csv.writer(file)
#         writer.writerows(dados)


path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'

# Extract

dados_empresaA = Dados(path_json)
dados_empresaB = Dados(path_csv)

print(f"Colunas dados Empresa A: {dados_empresaA.nome_colunas}")
print(f"QTD Colunas Empresa A: {dados_empresaA.qtd_linhas}")
print(f"Colunas dados Empresa B: {dados_empresaB.nome_colunas}")
print(f"QTD Colunas Empresa B: {dados_empresaB.qtd_linhas}")


# Transform

key_mapping = {'Nome do Item': 'Nome do Produto',
               'Classificação do Produto': 'Categoria do Produto',
               'Valor em Reais (R$)': 'Preço do Produto (R$)',
               'Quantidade em Estoque': 'Quantidade em Estoque',
               'Nome da Loja': 'Filial',
               'Data da Venda': 'Data da Venda'
               }

dados_empresaB.rename_columns(key_mapping)
print(f"Colunas dados Empresa B REFATORADO: {dados_empresaB.nome_colunas}")

dados_fusao = Dados.join(dados_empresaA, dados_empresaB)
print(f"Colunas dados FUSAO: {dados_fusao.nome_colunas}")
print(f"QTD Colunas FUSAO: {dados_fusao.qtd_linhas}")

# Load

path_dados_combinados = 'data_processed/dados_combinados.csv'
dados_fusao.salvando_dados(path_dados_combinados)
print(path_dados_combinados)


# # Iniciando a leitura
# dados_json = leitura_dados(path_json)
# nome_colunas_json = get_columns(dados_json)

# tamanho_dados_json = size_data(dados_json)
# print(f"Nome colunas dados JSON: {nome_colunas_json}")
# print(f"Tamanho dos dados JSON: {tamanho_dados_json}")

# dados_csv = leitura_dados(path_csv)
# nome_colunas_csv = get_columns(dados_csv)
# tamanho_dados_csv = size_data(dados_csv)
# print(f"Nome colunas dados CSV: {nome_colunas_csv}")
# print(f"Tamanho dos dados CSV: {tamanho_dados_csv}")


# # Transformação dos dados

# key_mapping = {'Nome do Item': 'Nome do Produto',
#                'Classificação do Produto': 'Categoria do Produto',
#                'Valor em Reais (R$)': 'Preço do Produto (R$)',
#                'Quantidade em Estoque': 'Quantidade em Estoque',
#                'Nome da Loja': 'Filial',
#                'Data da Venda': 'Data da Venda'
#                }


# dados_csv = rename_columns(dados_csv, key_mapping)
# nome_colunas_csv = get_columns(dados_csv)
# print(f"Nome colunas dados CSV TRATADOS: {nome_colunas_csv}")

# dados_fusao = join(dados_json,dados_csv)
# nome_colunas_fusao = get_columns(dados_fusao)
# tamanho_dados_fusao = size_data(dados_fusao)
# print(f"Nome colunas dados FUSAO: {nome_colunas_fusao}")
# print(f"Tamanho dos dados FUSAO: {tamanho_dados_fusao}")


# # Salvando os dados

# # Transformando lista de dicts em lista de listas
# dados_fusao_tabela = transformando_dados_tabela(dados_fusao, nome_colunas_fusao)
# print(f"Dados tratados DICT->LIST: {dados_fusao_tabela[:1]}")

# path_dados_combinados = 'data_processed/dados_combinados.csv'

# salvando_dados(path=path_dados_combinados, dados=dados_fusao_tabela)
# print(path_dados_combinados)
