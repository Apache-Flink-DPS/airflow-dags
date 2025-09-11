from datetime import datetime
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

@dag(start_date=datetime(2025, 9, 1), schedule=None, catchup=False)
def k8s_chained_loop():

    # 1. Generate a list of numbers
    def generate_numbers_callable(**context):
        return [1, 2, 3]

    generate_numbers = PythonOperator(
        task_id="generate_numbers_k8s_parallel_loop",
        python_callable=generate_numbers_callable,
    )

    # 2. Define KubernetesPodOperator template
    square = KubernetesPodOperator.partial(
        task_id="square",
        namespace="stefan-dev",
        name="square-task",
        image="python:3.12",
        cmds=["python", "-c"],
        arguments=[
            """
import json, os, sys
x = int(sys.argv[1])
print(f"Squaring {x}")
res = x * x

os.makedirs('/airflow/xcom', exist_ok=True)
with open('/airflow/xcom/return.json', 'w') as f:
    json.dump(res, f)
print(f"Result: {res}")
"""
        ],
        get_logs=True,
        do_xcom_push=True,
    )

    generate_numbers >> square

k8s_chained_loop()
