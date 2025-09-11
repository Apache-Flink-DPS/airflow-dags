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

with DAG(
        'complex_k8s_final',
        default_args=default_args,
        description='Complex DAG with only KubernetesPodOperator',
        schedule='@once',
        catchup=False,
        tags=['k8s', 'final'],
) as dag:

    csv_volume = k8s.V1Volume(
        name="csv-volume",
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
            claim_name="mypvc"
        )
    )

    volume_mount = k8s.V1VolumeMount(
        name="csv-volume",
        mount_path="/mnt/data"  # All tasks will access files here
    )

    # 1. Upload CSV to the shared volume
    upload_csv = KubernetesPodOperator(
        task_id='upload_csv',
        name='upload-csv-pod',
        namespace='stefan-dev',
        image='python:3.9-slim',
        cmds=['python', '-c'],
        arguments=[
            'import pandas as pd; '
            'df = pd.DataFrame({"col1": [1,2,3]}); '  # Example CSV content
            'df.to_csv("/mnt/data/raw.csv", index=False)'
        ],
        volumes=[csv_volume],
        volume_mounts=[volume_mount],
        in_cluster=True,
    )

    process_csv = KubernetesPodOperator(
        task_id='process_csv',
        name='process-csv-pod',
        namespace='stefan-dev',
        image='python:3.9-slim',
        cmds=['python', '-c'],
        arguments=[
            'import pandas as pd; '
            'df = pd.read_csv("/mnt/data/raw.csv"); '
            'df["new_col"] = df["col1"]*2; '
            'df.to_csv("/mnt/data/processed.csv", index=False)'
        ],
        volumes=[csv_volume],
        volume_mounts=[volume_mount],
        in_cluster=True,
    )

    process_processed_csv = KubernetesPodOperator(
        task_id='process_processed_csv',
        name='final-processing-pod',
        namespace='stefan-dev',
        image='python:3.9-slim',
        cmds=['python', '-c'],
        arguments=[
            'import pandas as pd; '
            'df = pd.read_csv("/mnt/data/processed.csv"); '
            'print("Final row count:", len(df)); '
            'print(df.head())'
        ],
        volumes=[csv_volume],
        volume_mounts=[volume_mount],
        in_cluster=True,
    )

    upload_csv >> process_csv >> process_processed_csv
