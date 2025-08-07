from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
import json

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
    'xcom_k8s_pipeline',
    default_args=default_args,
    description='Efficient DAG using XCom sidecar for data transfer',
    schedule='@once',
    catchup=False,
    tags=['example', 'efficient', 'xcom', 'k8s'],
)

def py_task_1(**kwargs):
    data = {"step": 1, "message": "Hello from Python task 1"}
    print(f"py_task_1 pushing: {data}")
    return data

py1 = PythonOperator(
    task_id='python_task_1',
    python_callable=py_task_1,
    dag=dag,
)

def py_task_2(**kwargs):
    ti = kwargs['ti']
    prev_data = ti.xcom_pull(task_ids='python_task_1')
    new_data = prev_data.copy()
    new_data['step'] = 2
    new_data['message'] = new_data['message'] + " -> modified by Python task 2"
    print(f"py_task_2 pushing: {new_data}")
    return new_data

py2 = PythonOperator(
    task_id='python_task_2',
    python_callable=py_task_2,
    dag=dag,
)

# This script will be executed in the K8s pod
k8s_script_1 = '''
import json
import os

# Read XCom data from mounted file (Airflow automatically mounts this)
try:
    with open('/airflow/xcom/return.json', 'r') as f:
        input_data = json.load(f)
    print(f"K8s task 1 received: {input_data}")
except FileNotFoundError:
    print("No XCom input found, starting fresh")
    input_data = {"step": 0, "message": "Starting from K8s"}

# Process the data
output_data = input_data.copy()
output_data['step'] = 3
output_data['message'] += " -> modified by k8s_task_1"

# Write output for XCom (Airflow automatically reads this)
os.makedirs('/airflow/xcom', exist_ok=True)
with open('/airflow/xcom/return.json', 'w') as f:
    json.dump(output_data, f)

print(f"K8s task 1 output: {output_data}")
'''

k8s_task_1 = KubernetesPodOperator(
    task_id='k8s_task_1',
    name='k8s-pod-1',
    namespace='stefan-dev',
    image='python:3.9-slim',
    cmds=['python', '-c'],
    arguments=[k8s_script_1],
    # Key settings for XCom - removed the invalid xcom_push parameter
    do_xcom_push=True,  # This is the correct parameter
    get_logs=True,
    in_cluster=True,
    is_delete_operator_pod=True,
    dag=dag,
)

k8s_script_2 = '''
import json
import os

# Read XCom data from previous task
try:
    with open('/airflow/xcom/return.json', 'r') as f:
        input_data = json.load(f)
    print(f"K8s task 2 received: {input_data}")
except FileNotFoundError:
    print("No XCom input found")
    input_data = {"step": 0, "message": "Error: no input"}

# Process the data
output_data = input_data.copy()
output_data['step'] = 4
output_data['message'] += " -> modified by k8s_task_2"

# Write output for XCom
os.makedirs('/airflow/xcom', exist_ok=True)
with open('/airflow/xcom/return.json', 'w') as f:
    json.dump(output_data, f)

print(f"K8s task 2 output: {output_data}")
'''

k8s_task_2 = KubernetesPodOperator(
    task_id='k8s_task_2',
    name='k8s-pod-2',
    namespace='stefan-dev',
    image='python:3.9-slim',
    cmds=['python', '-c'],
    arguments=[k8s_script_2],
    do_xcom_push=True,  # Only this parameter is needed
    get_logs=True,
    in_cluster=True,
    is_delete_operator_pod=True,
    dag=dag,
)

def final_py_task(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='k8s_task_2')
    print(f"Final Python task received: {data}")

py_final = PythonOperator(
    task_id='final_python_task',
    python_callable=final_py_task,
    dag=dag,
)

# Dependencies
py1 >> py2 >> k8s_task_1 >> k8s_task_2 >> py_final
