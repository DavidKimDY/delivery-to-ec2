from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime

def print_hello():
    print('hello')
    return 'hello'

def print_world():
    print('world')
    return 'world'

dag = DAG(
    dag_id='my_first_dag',
    start_date=datetime(2021,1,30),
    schedule_interval='20 4 * * *'
)

print_hello = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag
)

print_world = PythonOperator(
    task_id='print_world',
    python_callable=print_world,
    dag=dag
)

print_world >> print_hello