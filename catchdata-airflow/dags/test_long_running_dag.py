import time
from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def sleep_long():
    time.sleep(60 * 60)  # 1시간 sleep

with DAG(
    dag_id="test_long_running_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "monitoring"]
) as dag:

    long_task = PythonOperator(
        task_id="long_running_task",
        python_callable=sleep_long
    )
