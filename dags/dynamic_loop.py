from airflow.decorators import task, dag
from datetime import datetime

@dag(start_date=datetime(2025, 9, 1), schedule=None, catchup=False)
def chained_loop():

    @task
    def generate_numbers():
        return [1, 2, 3]

    @task
    def square(x: int):
        return x * x

    # dynamically expand based on generate_numbers output
    square.expand(x=generate_numbers())

chained_loop()
