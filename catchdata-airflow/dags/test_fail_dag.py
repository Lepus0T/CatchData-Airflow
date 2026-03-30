from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def always_fail():
    raise Exception("❌ 의도적으로 실패시키는 테스트 DAG")

with DAG(
    dag_id="test_fail_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # 수동 실행
    catchup=False,
    tags=["test", "monitoring"]
) as dag:

    fail_task = PythonOperator(
        task_id="fail_task",
        python_callable=always_fail
    )
