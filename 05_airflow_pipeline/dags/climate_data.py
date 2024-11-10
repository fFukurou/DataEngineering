from airflow import DAG
import pendulum
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.macros import ds_add

import pandas as pd
from personal_info import VISUAL_CROSSING_KEY


with DAG(
    'climate_data',
    start_date=pendulum.datetime(2024, 10, 28, tz="UTC"),
    schedule_interval='0 0 * * 1', # execute every monday --> CRON EXPRESSION
    # minute, hour, day_of_month, month, day_of_week
) as dag:
    task_1 = BashOperator(
        task_id = 'create_directory',
        bash_command = 'mkdir -p "/home/ffukurou/Documents/alura/Data_Engineering/05_airflow_pipeline/week={{data_interval_end.strftime("%Y-%m-%d")}}"'
    )
    
    def extract_data(data_interval_end):
        # Definindo as variables
        city = 'Boston'
        key = VISUAL_CROSSING_KEY
        URL = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}/{data_interval_end}/{ds_add(data_interval_end,7)}?unitGroup=metric&include=days&key={key}&contentType=csv"

        # Salvando os dados da URL (csv) em um dataframe
        dados = pd.read_csv(URL)

        # Pasta com o nome da semana/data
        file_path = f'/home/ffukurou/Documents/alura/Data_Engineering/05_airflow_pipeline/week={data_interval_end}/'

        # Separand as colunas em diferentes arquivos .csv e salvando eles dentro da pasta
        dados.to_csv(file_path + "dados_brutos.csv")
        dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperaturas.csv')
        dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')
        
    
    
    task_2 = PythonOperator(
        task_id = 'extract_data',
        python_callable= extract_data,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )
    
    task_1 >> task_2