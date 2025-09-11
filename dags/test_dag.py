from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
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

# Number of parallel instances for process_b
process_b_instances = [1, 2, 3]  # Adjust the list size for more parallel pods

with DAG(
        'complex_k8s_only_parallel_b',
        default_args=default_args,
        description='Complex DAG with multiple parallel process_b pods',
        schedule='@daily',
        catchup=False,
        tags=['k8s', 'complex'],
) as dag:

    # Group 1: Initialization
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

    # Group 2: Processing
    k8s_process_a = KubernetesPodOperator(
        task_id='process_a',
        name='process-a-pod',
        namespace='stefan-dev',
        image='python:3.9-slim',
        cmds=['python', '-c'],
        arguments=['import time; time.sleep(10); print("Processing task A")'],
        env_vars={'STAGE': 'A'},
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": 2, "memory": "200Mi"},
        ),
        in_cluster=True,
    )

    # Dynamic parallel instances of process_b
    k8s_process_b_mapped = KubernetesPodOperator.partial(
        task_id='process_b',
        name='process-b-pod',
        namespace='stefan-dev',
        image='python:3.9-slim',
        cmds=['python', '-c'],
        in_cluster=True,
    ).expand(
        arguments=[
            [f'import time; time.sleep(5); print("Processing task B instance {i}")']
            for i in process_b_instances
        ]
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

    # Group 3: Finalization
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
    init_hello >> init_date
    init_date >> k8s_process_a
    init_date >> k8s_process_b_mapped
    [k8s_process_a, k8s_process_b_mapped] >> k8s_process_c
    k8s_process_c >> k8s_finalize
