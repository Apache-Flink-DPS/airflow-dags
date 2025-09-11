from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.task_group import TaskGroup
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
        'complex_k8s_only',
        default_args=default_args,
        description='Complex DAG with only KubernetesPodOperator',
        schedule='@daily',
        catchup=False,
        tags=['k8s', 'complex'],
) as dag:

    # Group 1: Initialization
    with TaskGroup("init_group") as init_group:
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

        init_hello >> init_date

    # Group 2: Processing
    with TaskGroup("processing_group") as processing_group:
        k8s_process_a = KubernetesPodOperator(
            task_id='process_a',
            name='process-a-pod',
            namespace='stefan-dev',
            image='python:3.9-slim',
            cmds=['python', '-c'],
            arguments=['import time; time.sleep(10); print("Processing task A")'],
            env_vars={'STAGE': 'A'},
            container_resources=k8s.V1ResourceRequirements(
                requests={
                    "cpu": 2,
                    "memory": "200Mi",
                },
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
                requests={
                    "cpu": 1,
                    "memory": "200Mi",
                },
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
                requests={
                    "cpu": 10,
                    "memory": "200Mi",
                },
            ),
            in_cluster=True,
        )

        [k8s_process_a, k8s_process_b] >> k8s_process_c

    # Group 3: Finalization
    with TaskGroup("final_group") as final_group:
        k8s_finalize = KubernetesPodOperator(
            task_id='finalize',
            name='finalize-pod',
            namespace='stefan-dev',
            image='python:3.9-slim',
            cmds=['python', '-c'],
            arguments=['print("Pipeline completed successfully!")'],
            in_cluster=True,
        )

    # DAG flow
    init_group >> processing_group >> final_group
