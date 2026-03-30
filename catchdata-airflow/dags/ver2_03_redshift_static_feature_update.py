import io
import json
import math
from datetime import datetime, timedelta

import boto3
import joblib
import pandas as pd
import requests
from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,  # 테이블 생성용
)
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from sqlalchemy import Numeric, String

# =========================
# 기본 설정
# =========================
BUCKET_NAME = "team5-batch"

REDSHIFT_CONN_ID = "redshift_conn"
SCHEMA_NAME = "analytics"
RAW_TABLE = "raw_data.kakao_crawl"
FINAL_TABLE_NAME = "derived_features_base"

SLACK_WEBHOOK_URL = ("https://hooks.slack.com/services/T09SZ0BSHEU"
                     "/B0A3W3R4H9D/Ea5DqrFBnQKc3SzbSuNhcmZo")

# 가중치 설정 (base_population 계산용)
W_REVIEW = 1.0
W_BLOG = 0.7

# 24개 시간대 컬럼 이름 정의
TIME_COLUMNS = [f'time{i}' for i in range(24)]

# =========================
# 💡 SQL: 최종 테이블 생성 스키마
# =========================
# Redshift에 최적화된 테이블 스키마 정의
FINAL_TABLE_CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{FINAL_TABLE_NAME} (
    id BIGINT PRIMARY KEY,
    category VARCHAR(100),
    base_population NUMERIC(18, 4),
    quality_score NUMERIC(18, 4),
    rating NUMERIC(3, 2),
    -- 24개 시간대 컬럼 (방문자 수는 작으므로 SMALLINT 사용)
    {', '.join([f'{col} SMALLINT' for col in TIME_COLUMNS])},
    cluster SMALLINT,
    calculated_at TIMESTAMP
)
-- id를 기준으로 데이터 분산 및 정렬하여 조인 및 쿼리 성능 최적화
DISTKEY(id)
SORTKEY(calculated_at);
"""

# =========================
# 💡 보조 함수: S3에서 객체 로드
# =========================
def load_from_s3(bucket, key):
    # Airflow Connections에 설정된 AWS 자격증명을 사용하는 것이 좋으나,
    # 여기서는 직접 입력을 가정한 기본 구조로 작성합니다.
    s3 = boto3.client(
        "s3"
        )
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return joblib.load(io.BytesIO(response['Body'].read()))
    except s3.exceptions.NoSuchKey:
        # 에러 발생 시 버킷 내 파일 목록을 출력하여 경로를 확인하도록 유도
        print(f"❌ 에러: {bucket} 버킷에 {key} 파일이 없습니다.")
        objs = s3.list_objects_v2(Bucket=bucket, Prefix='models/') # models 폴더 목록 조회
        print("현재 존재하는 파일들:")
        for obj in objs.get('Contents', []):
            print(f" - {obj['Key']}")
        raise # 에러를 다시 던져서 Airflow에서 확인 가능하게 함



# =========================
# 💡 단일 통합 함수: 모든 로직을 순차적으로 실행 (Atomic Replacement)
# =========================
def full_static_feature_pipeline():
    """
    hourly_visit JSON을 24개 컬럼으로 변환하고,
    Redshift 테이블 이름 교체를 통해 원자적으로 갱신합니다.
    """

    # Redshift Hook 초기화
    redshift_hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
    engine = redshift_hook.get_sqlalchemy_engine()

    # 1. Redshift에서 원본 데이터 로드
    print("--- 1. Redshift에서 원본 데이터 로드 시작 ---")
    sql_select = f"""
    SELECT
        id,
        category_name,
        rating,
        review_count,
        blog_count,
        hourly_visit
    FROM {RAW_TABLE};
    """

    df = redshift_hook.get_pandas_df(sql_select)

    if df.empty:
        print(f"경고: {RAW_TABLE} 테이블에 데이터가 없습니다. 파이프라인을 종료합니다.")
        return

    print(f"✅ 원본 데이터 로드 완료: {len(df)}개")


    # 2. 파생 변수 계산 및 hourly_visit 분리 (Python/Pandas 환경)
    print("--- 2. 파생 변수 계산 및 hourly_visit 분리 시작 ---")

    # --- base_population 계산 ---
    df['base_population'] = (
        df['review_count'].apply(math.log1p) * W_REVIEW +
        df['blog_count'].apply(math.log1p) * W_BLOG
    )

    # --- quality_score 계산 ---
    df['quality_score'] = df['base_population'] * df['rating'].astype(float, errors='ignore')


    # --- hourly_visit JSON 파싱 및 24개 컬럼 분리 ---
    def safe_loads(json_str):
        """JSON 파싱 중 오류 발생 시 0으로 채워진 리스트를 반환"""
        try:
            if pd.isna(json_str) or json_str is None:
                return [0] * 24
            return json.loads(json_str)
        except Exception:
            # 리스트 길이가 24가 아닌 경우에도 0으로 채워진 리스트 반환
            return [0] * 24

    df['hourly_list'] = df['hourly_visit'].apply(safe_loads)

    # 24개 시간대별 컬럼 생성
    # Redshift의 작은 정수형(SMALLINT)으로 저장하기 위해 타입 변환
    df[TIME_COLUMNS] = pd.DataFrame(df['hourly_list'].to_list(), index=df.index).astype('int16')
    df.drop(columns=['hourly_list', 'hourly_visit'], inplace=True)

    # 클러스터링
    ## --- category 추출 ---
    df['category'] = df['category_name'].str.split('>').str[1].str.strip()

    ## --- 시간대별 방문 인원 나누기 ---
    df['breakfast'] = df[['time6','time7','time8','time9','time10']].sum(axis=1)
    df['lunch'] = df[['time11','time12','time13','time14','time15']].sum(axis=1)
    df['dinner'] = df[['time17','time18','time19','time20','time21']].sum(axis=1)
    df['late_night'] = df[['time21','time22','time23','time0','time1']].sum(axis=1)
    df['over_night'] = df[['time2','time3','time4','time5']].sum(axis=1)

    ## 원-핫 인코딩
    clustering_df = df[['id', 'category', 'base_population', 'quality_score',
                        'breakfast', 'lunch', 'dinner', 'late_night', 'over_night']]
    df_dummy = pd.get_dummies(clustering_df, columns=['category'], dtype=int)

    # 4. 모델 로드 및 예측
    print("--- ML 모델 로드 및 예측 시작 ---")
    model = load_from_s3(BUCKET_NAME, "models/kmeans_model_v1.pkl")
    scaler = load_from_s3(BUCKET_NAME, "models/scaler_v1.pkl")

    # [중요] 학습 시 사용했던 컬럼 리스트 로드 (컬럼 순서/개수 일치 필수)
    # 모델 저장 시 함께 저장했던 컬럼 리스트를 불러온다고 가정
    train_cols = load_from_s3(BUCKET_NAME, "models/train_columns.pkl")

    # 현재 데이터에 없는 카테고리 컬럼은 0으로 채우고, 학습 시 없던 컬럼은 제거
    for col in train_cols:
        if col not in df_dummy.columns:
            df_dummy[col] = 0

    # 학습 시와 동일한 컬럼 순서로 정렬 (id 제외)
    X = df_dummy[train_cols].drop(columns=['id'], errors='ignore')

    # 스케일링 및 예측
    X_scaled = scaler.transform(X)
    df['cluster'] = model.predict(X_scaled)

    # 5. 최종 데이터 정리
    final_df = df[['id', 'category', 'base_population', 'quality_score', 'rating',
                   *TIME_COLUMNS, 'cluster']].copy()
    final_df['calculated_at'] = datetime.now()


    print("✅ 파생 변수 및 시간대 컬럼 계산 완료")


    # 3. Redshift 테이블 이름 변경을 통한 원자적 교체
    print("--- 3. Redshift 테이블 이름 교체 시작 (Atomic Replacement) ---")

    # 💡 임시 테이블 및 백업 테이블 이름 정의
    STAGING_TABLE = 'derived_features_staging'
    BACKUP_TABLE = 'derived_features_old'

    # 💡 데이터 타입 매핑 정의 (Redshift SMALLINT로 매핑하기 위해 명시)
    dtype_mapping = {
        'category': String(50),
        'base_population': Numeric(18, 4),
        'quality_score': Numeric(18, 4),
        'rating': Numeric(3, 2),
        'cluster': Numeric(3, 0)
        # TIME_COLUMNS의 타입은 int16을 통해 SMALLINT로 자동으로 추론되도록 합니다.
    }

    # 3-1. 계산된 final_df를 임시 Staging 테이블에 로드
    final_df.to_sql(
        name=STAGING_TABLE,
        con=engine,
        schema=SCHEMA_NAME,
        if_exists='replace',
        index=False,
        dtype=dtype_mapping
    )

    print(f"   -> Staging 테이블 로드 완료: {SCHEMA_NAME}.{STAGING_TABLE}")

    # 3-2. Redshift 트랜잭션 시작 및 테이블 이름 교체 실행
    sql_commands = f"""
    BEGIN;

    -- 1. 기존 최종 테이블을 백업 테이블로 이름 변경
    ALTER TABLE {SCHEMA_NAME}.{FINAL_TABLE_NAME} RENAME TO {BACKUP_TABLE};

    -- 2. 임시 테이블을 최종 테이블 이름으로 변경 (원자적 교체)
    ALTER TABLE {SCHEMA_NAME}.{STAGING_TABLE} RENAME TO {FINAL_TABLE_NAME};

    COMMIT;

    -- 3. 이전 버전의 백업 테이블 정리
    DROP TABLE IF EXISTS {SCHEMA_NAME}.{BACKUP_TABLE};
    """


    redshift_hook.run(sql_commands)

    print(f"✅ {SCHEMA_NAME}.{FINAL_TABLE_NAME} 테이블이 {len(final_df)}개 레코드로 서비스 중단 없이 갱신되었습니다.")
    payload = {"text": ("*ver2_03_redshift_static_feature_update.py*\n"
                        f"📌 ✅ {SCHEMA_NAME}.{FINAL_TABLE_NAME} 테이블이 {len(final_df)}개 레코드로 서비스 중단 없이 갱신되었습니다.\n")}

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
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="ver2_03_redshift_static_feature_update",
    default_args=default_args,
    description="hourly_visit을 24개 time 컬럼으로 분리하고 RENAME을 통해 Redshift 테이블을 원자적으로 갱신합니다.",
    schedule=None,
    catchup=False
) as dag:

    # T0. 최종 테이블이 없는 경우 생성 (최초 실행 시 안정성 확보)
    t0_create_table = SQLExecuteQueryOperator(
        task_id="create_final_table_if_not_exists",
        conn_id=REDSHIFT_CONN_ID,
        sql=FINAL_TABLE_CREATE_SQL,
    )

    # T1. 데이터 로드, 계산 및 최종 테이블 갱신
    t1_full_pipeline = PythonOperator(
        task_id="run_full_static_feature_pipeline",
        python_callable=full_static_feature_pipeline,
    )

    # 파이프라인 흐름 정의: 테이블 생성 확인 후 데이터 갱신
    t0_create_table >> t1_full_pipeline
