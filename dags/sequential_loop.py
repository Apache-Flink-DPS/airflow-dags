from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG("k8s_sequential_loop", start_date=datetime(2025, 9, 1), schedule=None, catchup=False) as dag:
    previous = None
    for i in range(1, 4):
        task = KubernetesPodOperator(
            task_id=f"square_{i}",
            namespace="stefan-dev",
            name=f"square-task-{i}",
            image="python:3.12",
            cmds=["python", "-c"],
            arguments=[
                f"""
import json, os
x = {i}
res = x*x
os.makedirs('/airflow/xcom', exist_ok=True)
with open('/airflow/xcom/return.json','w') as f: json.dump(res, f)
print(f"Result: {{res}}")
"""
            ],
            get_logs=True,
            do_xcom_push=True,
        )
        if previous:
            previous >> task
        previous = task
