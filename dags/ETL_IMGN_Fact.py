# Airflow function
from airflow import DAG, Dataset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

import logging
from datetime import datetime, timedelta

from ETL_IMGN_Fact_game import etl_f_game_exchange
from ETL_Utils import init_clients, calc_target_date, target_date_range


ETL_IMGN_dimension = Dataset('ETL_IMGN_dimension')
ETL_IMGN_Fact      = Dataset('ETL_IMGN_Fact')

# 백필 설정 — None이면 정상 실행, 날짜 지정하면 백필 모드
BACKFILL_TARGET_DATE = None
# BACKFILL_TARGET_DATE = target_date_range("2026-05-08", "2026-05-10")


def etl_fact_imgn_game(**context):
    logger = logging.getLogger(__name__)

    client = init_clients()
    bq_client = client["bq_client"]

    if BACKFILL_TARGET_DATE is not None:
        target_date = BACKFILL_TARGET_DATE
    else:
        run_date = context.get('logical_date') or context.get('execution_date')
        if not run_date:
            run_date = datetime.now()
        target_date, _ = calc_target_date(run_date)

    print("✅✅✅✅ Calculated target_date:", target_date[0])

    try:
        etl_f_game_exchange(target_date=target_date, client=bq_client)
        logger.info("✅ etl_fact_imgn_game completed successfully")
        return True
    except Exception as e:
        logger.error(f"❌ etl_fact_imgn_game failed with error: {e}")
        raise e


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
    dag_id='ETL_IMGN_Fact',
    default_args=default_args,
    description='IMGN fact 테이블 ETL (KST D-1 기준)',
    schedule=[ETL_IMGN_dimension],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ETL', 'IMGN', 'fact'],
) as dag:

    etl_fact_imgn_game_task = PythonOperator(
        task_id='etl_f_game_exchange',
        python_callable=etl_fact_imgn_game,
    )

    bash_task = BashOperator(
        task_id='bash_task',
        outlets=[ETL_IMGN_Fact],
        bash_command='echo "ETL_IMGN_Fact 수행 완료"',
    )

    etl_fact_imgn_game_task >> bash_task
