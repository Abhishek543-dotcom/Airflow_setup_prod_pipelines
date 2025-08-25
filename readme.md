# 🌀 Apache Airflow + Kafka Streaming Integration

This project demonstrates how to use **Apache Airflow** in a containerized environment to orchestrate workflows that consume **real-time data from Kafka** and perform custom operations like logging, transformation, processing, or integration with downstream systems.

---

## 📦 Components

- **Kafka** – Message streaming system producing real-time data  
- **Apache Airflow** – Workflow orchestration engine (ETL, automation)  
- **Docker Compose** – Local environment for isolated testing and deployment  

---

## 🧱 Project Structure

```
.
├── dags/
│   ├── hello_airflow_dag.py       # Example DAG that prints a message
│   ├── kafka_consumer_dag.py      # DAG to consume Kafka messages
├── Dockerfile                     # Custom Airflow image (adds Kafka support)
├── docker-compose.yaml            # Brings up Airflow and dependencies
├── requirements.txt               # Python dependencies
└── README.md
```

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/airflow-kafka-integration.git
cd airflow-kafka-integration
```

---

### 2. Install Dependencies

Modify `requirements.txt` to include Python packages used in your DAGs.

```
kafka-python==2.0.2
```

> Add more as needed: pandas, requests, boto3, etc.

---

### 3. Build and Launch

```bash
docker compose up --build -d
```

Access Airflow UI at [http://localhost:8080](http://localhost:8080)

---

## 🛠️ Airflow Setup

### Default Credentials

- **Username**: `admin`  
- **Password**: `admin`

---

## 📂 Creating a New DAG

### Step-by-Step:

1. Add a new Python file in the `dags/` directory:

```bash
touch dags/my_custom_dag.py
```

2. Paste the following sample DAG:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def sample_task():
    print("✅ Hello from Airflow!")

with DAG(
    dag_id="my_custom_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    task = PythonOperator(
        task_id="print_hello",
        python_callable=sample_task
    )
```

3. Save the file and refresh the Airflow UI.

---

## ⚙️ Use Cases

This Airflow + Kafka setup can be extended for:

- ✅ Kafka-to-Database ingestion pipelines  
- ✅ Event-driven ETL transformations  
- ✅ Real-time log processing  
- ✅ API data pipelines  
- ✅ ML model retraining jobs  
- ✅ Batch or streaming analytics  

---

## 🧪 Example DAGs

### 1. Hello World DAG

A basic DAG to test Airflow installation:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print("✅ Hello, Airflow!")

with DAG(
    dag_id="hello_airflow_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",
    catchup=False
) as dag:
    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello
    )
```

### 2. Kafka Consumer DAG

Consumes a single message from Kafka:

```python
from kafka import KafkaConsumer
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def consume_kafka():
    consumer = KafkaConsumer(
        "order_events",
        bootstrap_servers="kafka:9092",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="airflow-consumer"
    )
    for message in consumer:
        print(f"Received: {message.value.decode('utf-8')}")
        break

with DAG(
    dag_id="kafka_consumer_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",
    catchup=False
) as dag:
    kafka_task = PythonOperator(
        task_id="consume_from_kafka",
        python_callable=consume_kafka
    )
```

---

## 📌 Tips

- Use **volumes** in Docker Compose to persist logs and DAGs  
- Always rebuild the image after editing `requirements.txt`:

```bash
docker compose build
```

- DAG files must be placed in the `dags/` directory  
- Restart services if new dependencies are added  

---

## 🧹 Stop & Clean Up

```bash
docker compose down
```

To remove volumes:

```bash
docker compose down -v
```

---

## 🧰 Future Improvements

- Slack/email alerting  
- Integration with databases or cloud storage  
- Unit testing DAGs with pytest  
- Sensor-based pipelines (file, database, API)

---

## 👨‍💻 Author

- Abhishek Tiwari

---

## 📄 License

MIT – free to use, modify, and distribute.
