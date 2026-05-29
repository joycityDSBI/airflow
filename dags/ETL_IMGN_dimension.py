# Airflow function
from airflow import DAG, Dataset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

from google.cloud import bigquery
import logging
from datetime import datetime, timezone, timedelta
import pytz

from ETL_Utils import init_clients, calc_target_date, target_date_range


ETL_ALL_Fact       = Dataset('ETL_ALL_Fact')
ETL_IMGN_dimension = Dataset('ETL_IMGN_dimension')

PROJECT_ID = "datahub-478802"
LOCATION   = "US"

# 백필 설정 — None이면 정상 실행, 날짜 지정하면 백필 모드
BACKFILL_TARGET_DATE = None
# BACKFILL_TARGET_DATE = target_date_range("2026-05-08", "2026-05-10")


def etl_dim_item_kind(**context):

    client = init_clients()["bq_client"]
    logger = logging.getLogger(__name__)
    kst = pytz.timezone('Asia/Seoul')

    if BACKFILL_TARGET_DATE is not None:
        target_date = BACKFILL_TARGET_DATE
    else:
        target_date, _ = calc_target_date(context['logical_date'])

    for td_str in target_date:
        try:
            current_date_obj = datetime.strptime(td_str, "%Y-%m-%d")
        except ValueError:
            print(f"⚠️ 날짜 형식이 잘못되었습니다: {td_str}")
            continue

        start_kst = kst.localize(current_date_obj)
        start_utc = start_kst.astimezone(timezone.utc)
        end_kst   = start_kst + timedelta(days=1)
        end_utc   = end_kst.astimezone(timezone.utc)

        print(f"📝 대상날짜: {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"   ㄴ 종료시간(UTC): {end_utc}")

        query = f"""
        MERGE `datahub-478802.IMGN.dim_item_kind` AS target
        USING (
            SELECT DISTINCT
                30007    AS joyple_game_code,
                p0       AS item_kind
            FROM `dataplatform-204306.IMGN.T_Log_Game`
            WHERE Reason IN (70001, 70002, 70003, 70004)
              AND LogDate_Svr >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
              AND LogDate_Svr <  TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
        ) AS source
        ON  target.joyple_game_code = source.joyple_game_code
        AND target.item_kind        = source.item_kind
        WHEN NOT MATCHED THEN
            INSERT (joyple_game_code, item_kind, update_timestamp)
            VALUES (source.joyple_game_code, source.item_kind, CURRENT_TIMESTAMP())
        """

        query_job = client.query(query)

        try:
            query_job.result()
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} dim_item_kind Batch 완료")
        except Exception as e:
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            raise e

    print("✅ dim_item_kind ETL 완료")
    return True


def adjust_dim_item_kind(**context):

    client = init_clients()["bq_client"]
    logger = logging.getLogger(__name__)

    if BACKFILL_TARGET_DATE is not None:
        target_date = BACKFILL_TARGET_DATE
    else:
        target_date, _ = calc_target_date(context['logical_date'])

    # TODO: item_name 매핑 source 테이블 미정 — 확정 후 아래 USING 절 채움
    query = f"""
    MERGE `datahub-478802.IMGN.dim_item_kind` AS target
    USING (
        -- TODO: item_name 매핑 테이블 확정 후 source 쿼리 작성
        SELECT
            CAST(NULL AS INT64)  AS joyple_game_code,
            CAST(NULL AS STRING) AS item_kind,
            CAST(NULL AS STRING) AS item_name
        WHERE FALSE
    ) AS source
    ON  target.joyple_game_code = source.joyple_game_code
    AND target.item_kind        = source.item_kind
    WHEN MATCHED THEN UPDATE SET
        target.item_name        = source.item_name,
        target.update_timestamp = CURRENT_TIMESTAMP()
    """

    query_job = client.query(query)

    try:
        query_job.result()
        print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
        print(f"■ adjust_dim_item_kind Batch 완료")
    except Exception as e:
        print(f"❌ 쿼리 실행 중 에러 발생: {e}")
        raise e

    print("✅ adjust_dim_item_kind 조정 완료")
    return True


# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
    dag_id='ETL_IMGN_dimension',
    default_args=default_args,
    description='IMGN dimension 테이블 ETL (KST D-1 기준)',
    schedule="00 22 * * *",  # KST 06:30 (UTC 21:30 전날)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ETL', 'IMGN', 'dim'],
) as dag:

    etl_dim_item_kind_task = PythonOperator(
        task_id='etl_dim_item_kind',
        python_callable=etl_dim_item_kind,
    )

    adjust_dim_item_kind_task = PythonOperator(
        task_id='adjust_dim_item_kind',
        python_callable=adjust_dim_item_kind,
    )

    bash_task = BashOperator(
        task_id='bash_task',
        outlets=[ETL_IMGN_dimension],
        bash_command='echo "ETL_IMGN_dimension 수행 완료"',
    )
    etl_dim_item_kind_task >> bash_task
    # etl_dim_item_kind_task >> adjust_dim_item_kind_task >> bash_task
