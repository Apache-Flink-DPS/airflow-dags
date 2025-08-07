from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
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
    'complex_xcom_k8s_pipeline',
    default_args=default_args,
    description='Complex DAG mixing Python and KubernetesPodOperator with XCom',
    schedule='@once',
    catchup=False,
    tags=['example', 'complex', 'xcom', 'k8s'],
)

# Step 1: Python → Python, produce dict in XCom
def py_task_1(**kwargs):
    ti = kwargs['ti']  # get task instance
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

# Step 3: Python → KubernetesPodOperator, pass data via XCom and env var
def make_env_from_xcom(ti):
    data = ti.xcom_pull(task_ids='python_task_2')
    # Pass as env var JSON string
    import json
    return [{'name': 'XCOM_DATA', 'value': json.dumps(data)}]

# KubernetesPodOperator cannot directly read XCom in pod; we pass via env var.

k8s_task_1 = KubernetesPodOperator(
    task_id='k8s_task_1',
    name='k8s-pod-1',
    namespace='stefan-dev',
    image='python:3.9-slim',
    cmds=['python', '-c'],
    arguments=[
        'import os, json; d=json.loads(os.environ["XCOM_DATA"]); '
        'd["step"]=3; d["message"]+=" -> modified by k8s_task_1"; '
        'print(json.dumps(d))'
    ],
    env_vars=make_env_from_xcom,
    get_logs=True,
    do_xcom_push=True,
    in_cluster=True,
    is_delete_operator_pod=True,
    dag=dag,
)

# Step 4: KubernetesPodOperator → KubernetesPodOperator, chain
def make_env_from_k8s_1(ti):
    data = ti.xcom_pull(task_ids='k8s_task_1')
    import json
    return [{'name': 'XCOM_DATA', 'value': json.dumps(data)}]

k8s_task_2 = KubernetesPodOperator(
    task_id='k8s_task_2',
    name='k8s-pod-2',
    namespace='stefan-dev',
    image='python:3.9-slim',
    cmds=['python', '-c'],
    arguments=[
        'import os, json; d=json.loads(os.environ["XCOM_DATA"]); '
        'd["step"]=4; d["message"]+=" -> modified by k8s_task_2"; '
        'print(json.dumps(d))'
    ],
    env_vars=make_env_from_k8s_1,
    get_logs=True,
    do_xcom_push=True,
    in_cluster=True,
    is_delete_operator_pod=True,
    dag=dag,
)

# Step 5: KubernetesPodOperator → PythonOperator, pull final XCom
def final_py_task(ti):
    data = ti.xcom_pull(task_ids='k8s_task_2')
    print(f"Final Python task received: {data}")

py_final = PythonOperator(
    task_id='final_python_task',
    python_callable=final_py_task,
    dag=dag,
)

# Dependencies
py1 >> py2 >> k8s_task_1 >> k8s_task_2 >> py_final
