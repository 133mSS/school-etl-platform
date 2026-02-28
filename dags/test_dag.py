from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Hello Data Engineering Project!")

with DAG(
    dag_id="test_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="print_hello",
        python_callable=hello,
    )

    task1