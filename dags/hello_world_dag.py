from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import time

def print_hello():
    return "Hello world from Airflow 3.0.2!"

def print_date():
    current_time = datetime.now()
    print(f"Current date and time: {current_time}")
    return "Date printed successfully"

def wait_sleep():
    time.sleep(20)
    return "Done"

default_args = {
    'owner': 'stefanpedratscher',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hello_world_traditional',
    default_args=default_args,
    description='A simple Hello World DAG using traditional operators',
    schedule='@daily',
    catchup=False,
    tags=['hello_world', 'traditional'],
)

start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

date_task = PythonOperator(
    task_id='print_date',
    python_callable=print_date,
    dag=dag,
)

sleep_task = PythonOperator(
    task_id='wait_sleep',
    python_callable=wait_sleep,
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end',
    dag=dag,
)

# Set task dependencies
start_task >> [hello_task, date_task, sleep_task] >> end_task
