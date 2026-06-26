# Airflow function
from airflow import DAG, Dataset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

import logging
from datetime import datetime, timedelta

from ETL_RESU_Fact_D2D import etl_f_d2d_item_flow
from ETL_Utils import init_clients, calc_target_date, target_date_range


ETL_RESU_Fact      = Dataset('ETL_RESU_Fact')
ETL_RESU_dimension = Dataset('ETL_RESU_dimension')

# 백필 설정 — None이면 정상 실행, 날짜 지정하면 백필 모드
BACKFILL_TARGET_DATE = None
# BACKFILL_TARGET_DATE = target_date_range("2025-09-03", "2025-09-05")


def etl_fact_resu_item_flow(**context):
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
        etl_f_d2d_item_flow(target_date=target_date, client=bq_client)
        logger.info("✅ etl_fact_resu_item_flow completed successfully")
        return True
    except Exception as e:
        logger.error(f"❌ etl_fact_resu_item_flow failed with error: {e}")
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
    dag_id='ETL_RESU_Fact',
    default_args=default_args,
    description='RESU fact 테이블 ETL (KST D-1 기준). ETL_RESU_dimension 완료 시 트리거',
    schedule=[ETL_RESU_dimension],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ETL', 'RESU', 'fact'],
) as dag:

    etl_f_d2d_item_flow_task = PythonOperator(
        task_id='etl_f_d2d_item_flow',
        python_callable=etl_fact_resu_item_flow,
    )

    bash_task = BashOperator(
        task_id='bash_task',
        outlets=[ETL_RESU_Fact],
        bash_command='echo "ETL_RESU_Fact 수행 완료"',
    )

    etl_f_d2d_item_flow_task >> bash_task
