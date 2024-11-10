from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

with DAG(
    "print_hello_world",
    start_date=days_ago(1),
    schedule_interval='@daily'
) as dag:

    def cumprimentos():
        print("Hello world, welcome to Airflow!")
        
        
    tarefa = PythonOperator(
        task_id = 'printHelloWorld',
        python_callable= cumprimentos
    )
    
    
    
