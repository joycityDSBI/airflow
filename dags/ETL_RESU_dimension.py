# Airflow function
from airflow import DAG, Dataset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

import logging
from datetime import datetime, timezone, timedelta
import pytz

from ETL_Utils import init_clients, calc_target_date, target_date_range


ETL_RESU_dimension = Dataset('ETL_RESU_dimension')

# 백필 설정 — None이면 정상 실행, 날짜 지정하면 백필 모드
BACKFILL_TARGET_DATE = None
# BACKFILL_TARGET_DATE = target_date_range("2025-09-03", "2025-09-05")


def _resolve_target_date(context):
    if BACKFILL_TARGET_DATE is not None:
        return BACKFILL_TARGET_DATE
    target_date, _ = calc_target_date(context['logical_date'])
    return target_date


def _kst_utc_window(td_str):
    kst = pytz.timezone('Asia/Seoul')
    current_date_obj = datetime.strptime(td_str, "%Y-%m-%d")
    start_kst = kst.localize(current_date_obj)
    start_utc = start_kst.astimezone(timezone.utc)
    end_utc   = (start_kst + timedelta(days=1)).astimezone(timezone.utc)
    return start_utc, end_utc


# ─────────────────────────────────────────────────────────────────────
# dim_item_kind
# ─────────────────────────────────────────────────────────────────────
def etl_dim_item_kind(**context):
    """로그에서 신규 item_kind 수집 → 키만 INSERT (메타는 adjust에서 채움)"""
    client = init_clients()["bq_client"]
    target_date = _resolve_target_date(context)

    for td_str in target_date:
        try:
            start_utc, end_utc = _kst_utc_window(td_str)
        except ValueError:
            print(f"⚠️ 날짜 형식이 잘못되었습니다: {td_str}")
            continue

        print(f"📝 대상날짜: {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"   ㄴ 종료시간(UTC): {end_utc}")

        query = f"""
        MERGE `datahub-478802.RESU.dim_item_kind` AS target
        USING (
            SELECT DISTINCT
                CAST(p1 AS INT64) AS item_kind
            FROM `dataplatform-204306.RESU.T_DM_Common`
            WHERE p1 IS NOT NULL
              AND LogDate >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
              AND LogDate <  TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
        ) AS source
        ON target.item_kind = source.item_kind
        WHEN NOT MATCHED THEN
            INSERT (item_kind, updated_at)
            VALUES (source.item_kind, CURRENT_TIMESTAMP())
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


# ─────────────────────────────────────────────────────────────────────
# dim_reason
# ─────────────────────────────────────────────────────────────────────
def etl_dim_reason(**context):
    """로그에서 신규 reason 수집 → 키만 INSERT (메타는 adjust에서 채움)"""
    client = init_clients()["bq_client"]
    target_date = _resolve_target_date(context)

    for td_str in target_date:
        try:
            start_utc, end_utc = _kst_utc_window(td_str)
        except ValueError:
            print(f"⚠️ 날짜 형식이 잘못되었습니다: {td_str}")
            continue

        print(f"📝 대상날짜: {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"   ㄴ 종료시간(UTC): {end_utc}")

        query = f"""
        MERGE `datahub-478802.RESU.dim_reason` AS target
        USING (
            SELECT DISTINCT
                CAST(Reason AS INT64) AS reason
            FROM `dataplatform-204306.RESU.T_DM_Common`
            WHERE Reason IS NOT NULL
              AND LogDate >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
              AND LogDate <  TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
        ) AS source
        ON target.reason = source.reason
        WHEN NOT MATCHED THEN
            INSERT (reason, updated_at)
            VALUES (source.reason, CURRENT_TIMESTAMP())
        """

        query_job = client.query(query)
        try:
            query_job.result()
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} dim_reason Batch 완료")
        except Exception as e:
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            raise e

    print("✅ dim_reason ETL 완료")
    return True


def adjust_dim_reason(**context):
    """dim_reason의 메타데이터 NULL row만 pre_resu_log_definition으로 채우는 UPDATE"""
    client = init_clients()["bq_client"]

    query = """
    MERGE `datahub-478802.RESU.dim_reason` T
    USING (
        SELECT
            t.reason,
            meta.reason_enum,
            meta.reason_desc,
            meta.p0_desc,
            CASE
                WHEN REGEXP_CONTAINS(UPPER(meta.reason_enum), r'GEM(GET|USE)')      THEN '젬'
                WHEN REGEXP_CONTAINS(UPPER(meta.reason_enum), r'ITEM(GET|USE)')     THEN '아이템'
                WHEN REGEXP_CONTAINS(UPPER(meta.reason_enum), r'RESOURCE(GET|USE)') THEN '자원'
                WHEN meta.reason_enum LIKE '%MEDALLION%'                            THEN '메달리온'
            END AS item_category,
            CASE
                WHEN REGEXP_CONTAINS(UPPER(meta.reason_enum), r'(GEM|ITEM|RESOURCE)GET') THEN '획득'
                WHEN REGEXP_CONTAINS(UPPER(meta.reason_enum), r'(GEM|ITEM|RESOURCE)USE') THEN '소비'
            END AS gain_or_spend_flag,
            REGEXP_CONTAINS(UPPER(IFNULL(meta.reason_enum, '')), r'ROLLBACK') AS is_rollback,
            meta.source_table,
            meta.source_column,
            CURRENT_TIMESTAMP() AS updated_at
        FROM `datahub-478802.RESU.dim_reason` t
        LEFT JOIN `datahub-478802.RESU.pre_resu_log_definition` meta
          ON t.reason = meta.reason
        WHERE t.reason_enum IS NULL
    ) S
    ON T.reason = S.reason
    WHEN MATCHED THEN UPDATE SET
        T.reason_enum        = S.reason_enum,
        T.reason_desc        = S.reason_desc,
        T.p0_desc            = S.p0_desc,
        T.gain_or_spend_flag = S.gain_or_spend_flag,
        T.item_category      = S.item_category,
        T.is_rollback        = S.is_rollback,
        T.source_table       = S.source_table,
        T.source_column      = S.source_column,
        T.updated_at         = S.updated_at
    """

    query_job = client.query(query)
    try:
        query_job.result()
        print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
        print("■ adjust_dim_reason Batch 완료")
    except Exception as e:
        print(f"❌ 쿼리 실행 중 에러 발생: {e}")
        raise e

    print("✅ adjust_dim_reason 조정 완료")
    return True


# ─────────────────────────────────────────────────────────────────────
# dim_action_detail
# ─────────────────────────────────────────────────────────────────────
def etl_dim_action_detail(**context):
    """로그에서 신규 (reason, p0) 조합 수집 → 키만 INSERT (메타는 adjust에서)"""
    client = init_clients()["bq_client"]
    target_date = _resolve_target_date(context)

    for td_str in target_date:
        try:
            start_utc, end_utc = _kst_utc_window(td_str)
        except ValueError:
            print(f"⚠️ 날짜 형식이 잘못되었습니다: {td_str}")
            continue

        print(f"📝 대상날짜: {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"   ㄴ 종료시간(UTC): {end_utc}")

        query = f"""
        MERGE `datahub-478802.RESU.dim_action_detail` AS target
        USING (
            SELECT DISTINCT
                CAST(reason AS INT64) AS reason,
                CAST(p0     AS INT64) AS act_detail_id
            FROM `dataplatform-204306.RESU.T_DM_Common`
            WHERE reason IS NOT NULL
              AND p0     IS NOT NULL
              AND reason IN (
                  SELECT reason
                  FROM `datahub-478802.RESU.dim_reason`
                  WHERE source_table IS NOT NULL
              )
              AND LogDate >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
              AND LogDate <  TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
        ) AS source
        ON  target.reason        = source.reason
        AND target.act_detail_id = source.act_detail_id
        WHEN NOT MATCHED THEN
            INSERT (reason, act_detail_id, updated_at)
            VALUES (source.reason, source.act_detail_id, CURRENT_TIMESTAMP())
        """

        query_job = client.query(query)
        try:
            query_job.result()
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} dim_action_detail Batch 완료")
        except Exception as e:
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            raise e

    print("✅ dim_action_detail ETL 완료")
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
    dag_id='ETL_RESU_dimension',
    default_args=default_args,
    description='RESU dimension 테이블 ETL (KST D-1 기준). Fact DAG의 트리거 소스.',
    schedule="20 21 * * *",  # KST 06:20 (Fact KST 06:30 이전)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ETL', 'RESU', 'dim'],
) as dag:

    etl_dim_item_kind_task = PythonOperator(
        task_id='etl_dim_item_kind',
        python_callable=etl_dim_item_kind,
    )

    etl_dim_reason_task = PythonOperator(
        task_id='etl_dim_reason',
        python_callable=etl_dim_reason,
    )

    adjust_dim_reason_task = PythonOperator(
        task_id='adjust_dim_reason',
        python_callable=adjust_dim_reason,
    )

    etl_dim_action_detail_task = PythonOperator(
        task_id='etl_dim_action_detail',
        python_callable=etl_dim_action_detail,
    )

    bash_task = BashOperator(
        task_id='bash_task',
        outlets=[ETL_RESU_dimension],
        bash_command='echo "ETL_RESU_dimension 수행 완료"',
    )

    # item_kind 브랜치 (메타데이터는 수동으로 채움 → adjust 없음)
    etl_dim_item_kind_task >> bash_task

    # reason → action_detail 브랜치 (action_detail은 dim_reason.source_table 필요)
    etl_dim_reason_task >> adjust_dim_reason_task >> etl_dim_action_detail_task >> bash_task
