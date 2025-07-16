from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define a simple Python function
def say_hello():
    print("Hello, Airflow!")

# Define the DAG
with DAG(
    dag_id="hello_airflow_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",  # runs daily
    catchup=False,
    tags=["example"]
) as dag:

    hello_task = PythonOperator(
        task_id="print_hello",
        python_callable=say_hello
    )
