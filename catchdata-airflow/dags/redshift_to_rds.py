import logging
from datetime import datetime, timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def transfer_redshift_to_rds(**context):
    """Redshift → RDS 전송 (UPSERT 방식)"""

    redshift_hook = PostgresHook(postgres_conn_id="redshift_conn")
    rds_hook = PostgresHook(postgres_conn_id="rds_conn")

    # 1. Redshift에서 데이터 추출
    logging.info("1. Redshift 데이터 추출")
    sql = """
        SELECT
            id, name, region, city, category, rating,
            phone, x, y, waiting, image_url, address,
            rec_quality, rec_balanced, rec_convenience, cluster
        FROM analytics.map_search
        ORDER BY id
    """

    records = redshift_hook.get_records(sql)
    record_count = len(records)
    logging.info(f"✓ {record_count:,}개 추출 완료")

    if not records:
        logging.warning("추출된 데이터가 없습니다")
        return

    # 2. RDS에 UPSERT
    logging.info(f"2. RDS UPSERT 시작 ({record_count:,}개)")

    conn = rds_hook.get_conn()
    cursor = conn.cursor()
    # postgresql은 대소문자 구별 못함
    # 따라서 컬럼명을 ""로 감싸줘야 대소문자 구별 가능
    try:
        cursor.executemany("""
            INSERT INTO main_restaurant (
                "restaurant_ID", name, region, city, category, rating, phone, x, y, waiting,
                image_url, address, rec_quality, rec_balanced, rec_convenience, cluster
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("restaurant_ID")
            DO UPDATE SET
                name = EXCLUDED.name,
                region = EXCLUDED.region,
                city = EXCLUDED.city,
                category = EXCLUDED.category,
                rating = EXCLUDED.rating,
                phone = EXCLUDED.phone,
                x = EXCLUDED.x,
                y = EXCLUDED.y,
                waiting = EXCLUDED.waiting,
                image_url = EXCLUDED.image_url,
                address = EXCLUDED.address,
                rec_quality = EXCLUDED.rec_quality,
                rec_balanced = EXCLUDED.rec_balanced,
                rec_convenience = EXCLUDED.rec_convenience,
                cluster=EXCLUDED.cluster
        """, records)

        conn.commit()
        logging.info(f"✓ {record_count:,}개 UPSERT 완료!")

    except Exception as e:
        conn.rollback()
        logging.error(f"✗ UPSERT 실패: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='redshift_to_rds_transfer',
    default_args=default_args,
    description='Redshift → RDS 데이터 전송 (UPSERT 방식)',
    schedule='30 3 * * 1',
    catchup=False,
    tags=['redshift', 'rds', 'upsert'],
) as dag:

    transfer_task = PythonOperator(
        task_id='transfer_data',
        python_callable=transfer_redshift_to_rds,
    )


# import logging
# from datetime import datetime, timedelta

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook


# def transfer_redshift_to_rds(**context):
#     """Redshift → RDS 전송 (UPSERT 방식)"""

#     redshift_hook = PostgresHook(postgres_conn_id="redshift_conn")
#     rds_hook = PostgresHook(postgres_conn_id="rds_conn")

#     # 1. Redshift에서 데이터 추출
#     logging.info("1. Redshift 데이터 추출")

#     # Redshift 컬럼 순서에 맞춰서 SELECT
#     sql = """
#         SELECT
#             id,                 -- 1
#             name,               -- 2
#             region,             -- 3
#             city,               -- 4
#             category,           -- 5
#             rating,             -- 6
#             phone,              -- 7
#             x,                  -- 8
#             y,                  -- 9
#             image_url,          -- 10
#             address,            -- 11
#             rec_quality,        -- 12
#             rec_balanced,       -- 13
#             rec_convenience     -- 14
#         FROM analytics.map_search
#         ORDER BY id
#         LIMIT 10
#     """

#     records = redshift_hook.get_records(sql)
#     record_count = len(records)
#     logging.info(f"✓ {record_count:,}개 추출 완료")

#     # 디버깅
#     if records:
#         first = records[0]
#         logging.info(f"🔍 첫 레코드 길이: {len(first)}개")
#         logging.info(f"🔍 첫 레코드: {first}")

#     if not records:
#         logging.warning("추출된 데이터가 없습니다")
#         return

#     # 2. RDS에 UPSERT
#     logging.info(f"2. RDS UPSERT 시작 ({record_count:,}개)")

#     conn = rds_hook.get_conn()
#     cursor = conn.cursor()

#     try:
#         # RDS 컬럼 순서에 맞춰서 INSERT (14개)
#         cursor.executemany("""
#             INSERT INTO main_restaurant (
#                 "restaurant_ID",
#                 name,
#                 region,
#                 city,
#                 category,
#                 rating,
#                 phone,
#                 x,
#                 y,
#                 waiting,
#                 image_url,
#                 address,
#                 rec_quality,
#                 rec_balanced,
#                 rec_convenience
#             ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#             ON CONFLICT ("restaurant_ID")
#             DO UPDATE SET
#                 name = EXCLUDED.name,
#                 region = EXCLUDED.region,
#                 city = EXCLUDED.city,
#                 category = EXCLUDED.category,
#                 rating = EXCLUDED.rating,
#                 phone = EXCLUDED.phone,
#                 x = EXCLUDED.x,
#                 y = EXCLUDED.y,
#                 waiting = EXCLUDED.waiting,
#                 image_url = EXCLUDED.image_url,
#                 address = EXCLUDED.address,
#                 rec_quality = EXCLUDED.rec_quality,
#                 rec_balanced = EXCLUDED.rec_balanced,
#                 rec_convenience = EXCLUDED.rec_convenience
#         """, records)

#         conn.commit()
#         logging.info(f"✓ {record_count:,}개 UPSERT 완료!")

#     except Exception as e:
#         conn.rollback()
#         logging.error(f"✗ UPSERT 실패: {e}")
#         logging.error(f"🔍 에러 발생한 레코드: {records[0] if records else 'None'}")
#         raise
#     finally:
#         cursor.close()
#         conn.close()


# # DAG 정의
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 12, 19),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# with DAG(
#     dag_id='redshift_to_rds_transfer',
#     default_args=default_args,
#     description='Redshift → RDS 데이터 전송 (UPSERT 방식)',
#     schedule='30 3 * * 1',
#     catchup=False,
#     tags=['redshift', 'rds', 'upsert'],
# ) as dag:

#     transfer_task = PythonOperator(
#         task_id='transfer_data',
#         python_callable=transfer_redshift_to_rds,
#     )
