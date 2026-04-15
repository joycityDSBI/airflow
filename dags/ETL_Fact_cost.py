from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable

from google.cloud import bigquery
import logging
from datetime import datetime, timedelta

from ETL_Fact_tracker import etl_f_cost_campaign_rule
from ETL_Utils import init_clients


def run_etl_cost_campaign_rule(**context):
    logger = logging.getLogger(__name__)

    client = init_clients()
    bq_client = client["bq_client"]

    try:
        etl_f_cost_campaign_rule(client=bq_client)
        logger.info("✅ etl_f_cost_campaign_rule completed successfully")
        return True
    except Exception as e:
        logger.error(f"❌ etl_f_cost_campaign_rule failed with error: {e}")
        raise e


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    dag_id='ETL_Fact_cost',
    default_args=default_args,
    description='f_cost_campaign_rule 테이블 매시간 전체 재적재',
    schedule='0 * * * *',  # 매시간 정각 (UTC)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ETL', 'fact', 'cost', 'bigquery'],
) as dag:

    etl_cost_campaign_rule_task = PythonOperator(
        task_id='etl_cost_campaign_rule',
        python_callable=run_etl_cost_campaign_rule,
    )
