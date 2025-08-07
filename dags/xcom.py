from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

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
    'k8s_xcom_example',
    default_args=default_args,
    description='DAG with two KubernetesPodOperator tasks sharing data via XCom',
    schedule='@once',
    catchup=False,
    tags=['example', 'k8s', 'xcom'],
)

push_task = KubernetesPodOperator(
    task_id='push_task',
    name='push-pod',
    namespace='stefan-dev',
    image='python:3.9-slim',
    cmds=['python', '-c'],
    # Print JSON string to stdout — KubernetesPodOperator captures stdout as XCom if do_xcom_push=True
    arguments=['import json; print(json.dumps({"message": "Hello from push_task", "value": 123}))'],
    get_logs=True,
    do_xcom_push=True,
    in_cluster=True,
    is_delete_operator_pod=True,
    dag=dag,
)

def pull_python_callable(ti):
    data = ti.xcom_pull(task_ids='push_task')
    print(f"Pulled from XCom: {data}")

# For pulling, simplest is a PythonOperator:
from airflow.operators.python import PythonOperator

pull_task = PythonOperator(
    task_id='pull_task',
    python_callable=pull_python_callable,
    dag=dag,
)

push_task >> pull_task
