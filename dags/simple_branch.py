from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from kubernetes.client import models as k8s

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
        'complex_k8s_branching',
        default_args=default_args,
        description='Complex DAG with branching and KubernetesPodOperator',
        schedule='@daily',
        catchup=False,
        tags=['k8s', 'branching'],
) as dag:

    # --- Initialization ---
    init_hello = KubernetesPodOperator(
        task_id='init_hello',
        name='init-hello-pod',
        namespace='stefan-dev',
        image='python:3.9-slim',
        cmds=['python', '-c'],
        arguments=['print("Initializing pipeline...")'],
        in_cluster=True,
    )

    init_date = KubernetesPodOperator(
        task_id='init_date',
        name='init-date-pod',
        namespace='stefan-dev',
        image='python:3.9-slim',
        cmds=['python', '-c'],
        arguments=['import datetime; print("Init time:", datetime.datetime.now())'],
        in_cluster=True,
    )

    # --- Branching decision ---
    def branch_func(**context):
        """Simple branching logic: choose path based on current minute."""
        minute = datetime.now().minute
        if minute % 2 == 0:  # even minute → process A+B path
            return ['process_a', 'process_b']
        else:  # odd minute → skip directly to process_c
            return 'process_c'

    branch_op = BranchPythonOperator(
        task_id='branch_decision',
        python_callable=branch_func,
        provide_context=True,
    )

    # --- Processing ---
    k8s_process_a = KubernetesPodOperator(
        task_id='process_a',
        name='process-a-pod',
        namespace='stefan-dev',
        image='python:3.9-slim',
        cmds=['python', '-c'],
        arguments=['import time; time.sleep(5); print("Processing task A")'],
        env_vars={'STAGE': 'A'},
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": 2, "memory": "200Mi"},
        ),
        in_cluster=True,
    )

    k8s_process_b = KubernetesPodOperator(
        task_id='process_b',
        name='process-b-pod',
        namespace='stefan-dev',
        image='python:3.9-slim',
        cmds=['python', '-c'],
        arguments=['print("Processing task B")'],
        env_vars={'STAGE': 'B'},
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": 1, "memory": "200Mi"},
        ),
        in_cluster=True,
    )

    k8s_process_c = KubernetesPodOperator(
        task_id='process_c',
        name='process-c-pod',
        namespace='stefan-dev',
        image='python:3.9-slim',
        cmds=['python', '-c'],
        arguments=['import time; time.sleep(10); print("Finished C!!")'],
        env_vars={'STAGE': 'C'},
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": 6, "memory": "200Mi"},
        ),
        in_cluster=True,
    )

    # --- Finalization ---
    k8s_finalize = KubernetesPodOperator(
        task_id='finalize',
        name='finalize-pod',
        namespace='stefan-dev',
        image='python:3.9-slim',
        cmds=['python', '-c'],
        arguments=['print("Pipeline completed successfully!")'],
        in_cluster=True,
        trigger_rule="none_failed_min_one_success",  # important for branching
    )

    # DAG Flow
    init_hello >> init_date >> branch_op
    branch_op >> [k8s_process_a, k8s_process_b, k8s_process_c]
    [k8s_process_a, k8s_process_b, k8s_process_c] >> k8s_finalize
