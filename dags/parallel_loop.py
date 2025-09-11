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
        task_id="generate_numbers",
        python_callable=generate_numbers_callable,
    )

    # 2. Define KubernetesPodOperator template
    square = KubernetesPodOperator.partial(
        task_id="square",
        namespace="default",
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

    # 3. Dynamically expand the K8s operator over the list from generate_numbers
    square.expand(arguments=generate_numbers.output.map(lambda n: ["python", "-c", f"import json, os; x={n}; res=x*x; os.makedirs('/airflow/xcom', exist_ok=True); json.dump(res, open('/airflow/xcom/return.json','w')); print(res)"]))

k8s_chained_loop()
