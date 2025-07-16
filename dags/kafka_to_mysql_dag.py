from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kafka import KafkaConsumer
import json
import pymysql

default_args = {
    'start_date': datetime(2023, 1, 1),
    'catchup': False,
}

def consume_and_insert():
    consumer = KafkaConsumer(
        'order-events',
        bootstrap_servers='host.docker.internal:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='root',
        database='airflow'
    )

    cursor = connection.cursor()
    for msg in consumer:
        order = msg.value
        cursor.execute("""
            INSERT INTO orders (order_id, user_id, amount, currency, status, order_time)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            order['order_id'],
            order['user_id'],
            order['order_amount'],
            order['currency'],
            order['order_status'],
            order['order_time']
        ))
        connection.commit()
        break  # remove this if you want it to keep consuming

    cursor.close()
    connection.close()

with DAG("kafka_to_mysql", default_args=default_args, schedule_interval="@hourly") as dag:
    t1 = PythonOperator(
        task_id='consume_and_insert',
        python_callable=consume_and_insert
    )
