import random
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

# =========================
# 기본 설정
# =========================
REDSHIFT_CONN_ID = "redshift_conn"
SCHEMA_NAME = "analytics"

BASE_TABLE = f"{SCHEMA_NAME}.derived_features_base"
REALTIME_TABLE = f"{SCHEMA_NAME}.realtime_waiting"
FINAL_TABLE_NAME = "realtime_waiting"
SLACK_WEBHOOK_URL = ("https://hooks.slack.com/services/T09SZ0BSHEU"
                     "/B0A3W3R4H9D/Ea5DqrFBnQKc3SzbSuNhcmZo")

# ── 추천 전략 가중치 ─────────────────
QUALITY_W1, QUALITY_W2, QUALITY_W3 = 0.6, 0.2, 0.2
BALANCED_W1, BALANCED_W2, BALANCED_W3 = 0.4, 0.35, 0.25
CONVENIENCE_W1, CONVENIENCE_W2, CONVENIENCE_W3 = 0.3, 0.2, 0.5


# =========================
# 실시간 테이블 생성 SQL
# =========================
REALTIME_TABLE_CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {REALTIME_TABLE} (
    id BIGINT,
    current_visitors NUMERIC(18, 4),
    waiting INTEGER,
    rec_quality NUMERIC(18, 6),
    rec_balanced NUMERIC(18, 6),
    rec_convenience NUMERIC(18, 6),
    calculation_timestamp TIMESTAMP
)
DISTKEY(id)
SORTKEY(calculation_timestamp);
"""


# =========================
# 실시간 계산 파이프라인
# =========================
def calculate_realtime_scores():
    KST = timezone(timedelta(hours=9))
    now = pd.Timestamp(datetime.now(KST))
    now_hour = now.hour
    TIME_COLUMN = f"time{now_hour}"
    redshift_hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
    engine = redshift_hook.get_sqlalchemy_engine()

    # 1. 정적 피처 로드
    sql = f"""
        SELECT
            id,
            base_population,
            quality_score,
            rating,
            {TIME_COLUMN}
        FROM {BASE_TABLE};
    """
    df = redshift_hook.get_pandas_df(sql)

    if df.empty:
        print("⚠️ base 테이블 비어 있음 → 종료")
        return

    # 2. 시간대 비율 계산
    max_visits = df[TIME_COLUMN].max()
    min_visits = df[TIME_COLUMN].min()
    divisor = max(max_visits - min_visits, 1)

    def calc(row):
        hour_ratio = (row[TIME_COLUMN] - min_visits) / divisor
        virtual_base = row['base_population'] * 2.5
        # current_visitors = row["base_population"] * hour_ratio
        current_visitors = virtual_base * hour_ratio

        # 방문자 거의 없으면 대기 없음
        if current_visitors < 1:
            return pd.Series([current_visitors, 0])

        base_waiting_ratio = 0.2 + (hour_ratio ** 2) * 0.8
        expected_waiting = current_visitors * base_waiting_ratio
        # expected_waiting = current_visitors * (0.15 + hour_ratio * 0.25)
        variation = expected_waiting * 0.5

        waiting = int(random.normalvariate(expected_waiting, variation))
        waiting = max(0, min(waiting, int(current_visitors)))

        return pd.Series([current_visitors, waiting])

    df[["current_visitors", "waiting"]] = df.apply(calc, axis=1)

    # 3. 정규화
    df["quality_norm"] = df["quality_score"] / (df["quality_score"] + 10)
    df["visitors_norm"] = df["current_visitors"] / (df["current_visitors"] + 20)
    df["waiting_norm"] = df["waiting"] / (df["waiting"] + 10)

    # 4. 추천 점수 (3가지 전략)
    df["rec_quality"] = (
        QUALITY_W1 * df["quality_norm"]
        + QUALITY_W2 * df["visitors_norm"]
        - QUALITY_W3 * df["waiting_norm"]
    )

    df["rec_balanced"] = (
        BALANCED_W1 * df["quality_norm"]
        + BALANCED_W2 * df["visitors_norm"]
        - BALANCED_W3 * df["waiting_norm"]
    )

    df["rec_convenience"] = (
        CONVENIENCE_W1 * df["quality_norm"]
        + CONVENIENCE_W2 * df["visitors_norm"]
        - CONVENIENCE_W3 * df["waiting_norm"]
    )

    final_df = df[
        [
            "id",
            "current_visitors",
            "waiting",
            "rec_quality",
            "rec_balanced",
            "rec_convenience",
        ]
    ].copy()

    final_df["calculation_timestamp"] = now

    # 5. TEMP 테이블 로드
    TEMP_TABLE = "realtime_waiting_temp"
    BACKUP_TABLE = 'realtime_waiting_old'

    final_df.to_sql(
        name=TEMP_TABLE,
        con=engine,
        schema=SCHEMA_NAME,
        if_exists="replace",
        index=False,
    )

    # 6. 안전한 UPSERT (timestamp 캐스팅!)
    # Redshift 트랜잭션 시작 및 테이블 이름 교체 실행
    sql_commands = f"""
    BEGIN;

    -- 1. 기존 최종 테이블을 백업 테이블로 이름 변경
    ALTER TABLE {SCHEMA_NAME}.{FINAL_TABLE_NAME} RENAME TO {BACKUP_TABLE};

    -- 2. 임시 테이블을 최종 테이블 이름으로 변경 (원자적 교체)
    ALTER TABLE {SCHEMA_NAME}.{TEMP_TABLE} RENAME TO {FINAL_TABLE_NAME};

    COMMIT;

    -- 3. 이전 버전의 백업 테이블 정리
    DROP TABLE IF EXISTS {SCHEMA_NAME}.{BACKUP_TABLE};
    """


    redshift_hook.run(sql_commands)

    print("*ver2_04_calculate_realtime_scores.py*\n"
          f"✅ realtime_waiting UPSERT 완료: {len(final_df)} rows")
    payload = {"text": ("*ver2_04_calculate_realtime_scores.py*\n"
                        f"📌 ✅ realtime_waiting UPSERT 완료: {len(final_df)} rows\n")}

    requests.post(
        SLACK_WEBHOOK_URL,
        json=payload,
        timeout=10,
    )

# =========================
# DAG 정의
# =========================
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ver2_04_calculate_realtime_scores",
    default_args=default_args,
    schedule=None,  # 또는 원하는 주기
    catchup=False,
) as dag:

    t0_create_table = SQLExecuteQueryOperator(
        task_id="create_realtime_table_if_not_exists",
        conn_id=REDSHIFT_CONN_ID,
        sql=REALTIME_TABLE_CREATE_SQL,
    )

    t1_calculate = PythonOperator(
        task_id="calculate_and_upsert_realtime_scores",
        python_callable=calculate_realtime_scores,
    )

    # # 💡 추가: 다음 DAG를 실행시키는 태스크
    # t2_trigger_map_search = TriggerDagRunOperator(
    #     task_id="trigger_map_search_dag",
    #     trigger_dag_id="map_search", # 실행시키고자 하는 상대 DAG의 dag_id
    #     wait_for_completion=False, # 이 DAG가 상대방이 끝날 때까지 기다릴지 여부
    #     poke_interval=60
    # )

    # 파이프라인 흐름 업데이트
    # t0_create_table >> t1_calculate >> t2_trigger_map_search
    t0_create_table >> t1_calculate
