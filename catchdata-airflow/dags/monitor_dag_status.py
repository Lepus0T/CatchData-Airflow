from datetime import datetime, timedelta, timezone

import pendulum
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable

# =========================
# 설정값
# =========================
SLACK_WEBHOOK_URL = Variable.get("SLACK_WEBHOOK_URL")

CHECK_INTERVAL_MIN = 60        # 최근 1시간
RUNNING_THRESHOLD_MIN = 30    # 30분 이상 running

# =========================
# 모니터링 로직
# =========================
def monitor_dags():
    hook = PostgresHook(postgres_conn_id="airflow_db")
    conn = hook.get_conn()
    cur = conn.cursor()

    # 1️⃣ 실패한 DAG (dag_run → logical_date)
    cur.execute("""
        SELECT dag_id, logical_date
        FROM dag_run
        WHERE state = 'failed'
        AND logical_date >= NOW() - INTERVAL %s
        ORDER BY logical_date DESC
    """, (f"{CHECK_INTERVAL_MIN} minutes",))
    failed_dags = cur.fetchall()

    # 2️⃣ 실패한 Task (task_instance → start_date)
    cur.execute("""
        SELECT dag_id, task_id, start_date
        FROM task_instance
        WHERE state = 'failed'
        AND start_date >= NOW() - INTERVAL %s
        ORDER BY start_date DESC
    """, (f"{CHECK_INTERVAL_MIN} minutes",))
    failed_tasks = cur.fetchall()

    # 3️⃣ 장시간 running DAG (dag_run → start_date)
    cur.execute("""
        SELECT dag_id, start_date
        FROM dag_run
        WHERE state = 'running'
        AND start_date <= NOW() - INTERVAL %s
        ORDER BY start_date
    """, (f"{RUNNING_THRESHOLD_MIN} minutes",))
    long_running_dags = cur.fetchall()

    cur.close()
    conn.close()

    if not failed_dags and not failed_tasks and not long_running_dags:
        return  # 알림 보낼 게 없으면 종료

    # =========================
    # Slack 메시지 구성
    # =========================
    KST = pendulum.timezone("Asia/Seoul")

    def to_kst(dt):
        """
        UTC datetime → KST datetime 문자열
        """
        if dt is None:
            return "-"
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")

    message = "*🚨 Airflow DAG 모니터링 알림*\n\n"

    if failed_dags:
        message += "❌ *실패한 DAG (최근 1시간)*\n"
        for dag_id, logical_date in failed_dags:
            message += f"• `{dag_id}` @ {to_kst(logical_date)}\n"
        message += "\n"

    if failed_tasks:
        message += "🧩 *실패한 Task (최근 1시간)*\n"
        for dag_id, task_id, start_date in failed_tasks:
            message += f"• `{dag_id}.{task_id}` @ {to_kst(start_date)}\n"
        message += "\n"

    if long_running_dags:
        message += "🕒 *30분 이상 실행 중인 DAG*\n"
        for dag_id, start_date in long_running_dags:
            message += f"• `{dag_id}` (시작: {to_kst(start_date)})\n"
        message += "\n"

    # =========================
    # Slack 전송
    # =========================
    resp = requests.post(
        SLACK_WEBHOOK_URL,
        json={"text": message},
        timeout=10
    )

    if resp.status_code != 200:
        raise RuntimeError(f"Slack webhook failed: {resp.text}")

# =========================
# DAG 정의
# =========================
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_monitoring",
    description="Airflow DAG 상태 모니터링 (실패 / 장기 실행)",
    start_date=datetime(2025, 1, 1),
    schedule="*/10 * * * *",  # 10분마다
    catchup=False,
    default_args=default_args,
    tags=["monitoring", "slack"],
) as dag:

    monitor_task = PythonOperator(
        task_id="monitor_dag_status",
        python_callable=monitor_dags,
    )
