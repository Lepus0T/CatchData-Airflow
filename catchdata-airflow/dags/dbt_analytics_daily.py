from datetime import datetime, timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt_analytics_daily",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=KST),
    schedule="0 4 * * 1",  # 매주 월요일 04:00 (KST)
    catchup=False,
    tags=["dbt", "analytics"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        cd /opt/airflow/dbt/catchdata_dbt && \
        dbt run
        """,
    )
