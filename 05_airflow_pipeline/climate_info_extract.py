import os
from os.path import join
import pandas as pd
from datetime import datetime, timedelta
from personal_info import VISUAL_CROSSING_KEY

# Data interval
data_inicio = datetime.today()
data_fim = data_inicio + timedelta(days=7)

# Formatando as datas
data_inicio = data_inicio.strftime('%Y-%m-%d')
data_fim = data_fim.strftime('%Y-%m-%d')

# Definindo as variables
city = 'Boston'
key = VISUAL_CROSSING_KEY
URL = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}/{data_inicio}/{data_fim}?unitGroup=metric&include=days&key={key}&contentType=csv"

# Salvando os dados da URL (csv) em um dataframe
dados = pd.read_csv(URL)
print(dados.head())

# Criando uma pasta com o nome da semana/data
file_path = f"semana={data_inicio}/"
os.mkdir(file_path)

# Separand as colunas em diferentes arquivos .csv e salvando eles dentro da pasta
dados.to_csv(file_path + "dados_brutos.csv")
dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperaturas.csv')
dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')
