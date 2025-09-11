from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

default_args = {
    'owner': 'stefanpedratscher',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
        'simple_branch',
        default_args=default_args,
        description='Complex DAG with only branch',
        schedule='@daily',
        catchup=False,
        tags=['branch', 'simple'],
) as dag:
    @task.branch(task_id="branch_task")
    def branch_func(ti=None):
        xcom_value = int(ti.xcom_pull(task_ids="k8s_task_1"))
        if xcom_value >= 5:
            return ['continue_task', 'continue_task2']
        elif xcom_value >= 3:
            return ["stop_task"]
        else:
            return None

    k8s_task_1 = KubernetesPodOperator(
        task_id='k8s_task_1',
        name='k8s-pod-1',
        namespace='stefan-dev',
        image='python:3.9-slim',
        cmds=['python', '-c'],
        arguments=['''
import json
import os

# XCom data injected via template
input_data = {{ ti.xcom_pull(task_ids='start_task') | tojson }}
print(f"K8s task 1 received: {input_data}")

# Process the data
output_data = int(input_data) + 1

# Write output for XCom
os.makedirs('/airflow/xcom', exist_ok=True)
with open('/airflow/xcom/return.json', 'w') as f:
    json.dump(output_data, f)

print(f"K8s task 1 output: {output_data}")
    '''],
        do_xcom_push=True,
        get_logs=True,
        in_cluster=True,
        is_delete_operator_pod=True,
        dag=dag,
    )

    start_op = BashOperator(
        task_id="start_task",
        bash_command="echo 4",
        do_xcom_push=True,
        dag=dag,
    )

    branch_op = branch_func()

    continue_op = EmptyOperator(task_id="continue_task", dag=dag)
    continue_op2 = EmptyOperator(task_id="continue_task2", dag=dag)
    stop_op = EmptyOperator(task_id="stop_task", dag=dag)

    start_op >> k8s_task_1 >> branch_op >> [continue_op, continue_op2, stop_op]
