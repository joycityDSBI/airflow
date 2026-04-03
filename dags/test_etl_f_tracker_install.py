from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable

from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timezone, timedelta
import pytz
import json
import logging


# ── 설정 ─────────────────────────────────────────────────────────────────────
PROJECT_ID = "datahub-478802"
LOCATION = "US"


# ── BQ 클라이언트 초기화 ───────────────────────────────────────────────────────
def init_bq_client():
    credentials_json = Variable.get('GOOGLE_CREDENTIAL_JSON')
    cred_dict = json.loads(credentials_json)
    if 'private_key' in cred_dict:
        cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
    SCOPES = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/bigquery",
    ]
    creds = service_account.Credentials.from_service_account_info(cred_dict, scopes=SCOPES)
    return bigquery.Client(project=PROJECT_ID, credentials=creds)


# ── 테스트 대상 함수 ───────────────────────────────────────────────────────────
def etl_f_tracker_install(target_date: list, client):

    kst = pytz.timezone('Asia/Seoul')

    for td_str in target_date:
        try:
            current_date_obj = datetime.strptime(td_str, "%Y-%m-%d")
        except ValueError:
            print(f"⚠️ 날짜 형식이 잘못되었습니다: {td_str}")
            continue

        start_kst = kst.localize(current_date_obj)
        start_utc = start_kst.astimezone(timezone.utc)
        end_kst = start_kst + timedelta(days=1)
        end_utc = end_kst.astimezone(timezone.utc)

        print(f"📝 대상날짜: {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"   ㄴ 종료시간(UTC): {end_utc}")

        start_utc_str = start_utc.strftime("%Y-%m-%d %H:%M:%S")
        end_utc_str = end_utc.strftime("%Y-%m-%d %H:%M:%S")

        # ── 쿼리를 여기에 작성하세요 ──────────────────────────────────────────
        query = f"""
        MERGE `datahub-478802.datahub.f_tracker_install_test` as target
        USING
        (
                SELECT 
                AppID as app_id,
                joypleGameID as joyple_game_code,
                marketID as market_id,
                trackerAccountID as tracker_account_id,
                trackerTypeID as tracker_type_id,
                BundleID as bundle_id,
                Platform as platform,
                CountryCode as country_code,
                MediaSource as media_source,
                MediaSourceCat as media_source_cat,
                IsOrganic as is_organic,
                Agency as agency,
                Campaign as campaign,
                InitCampaign as init_campaign,
                AdsetName as adset_name,
                AdName as ad_name,
                IsRetargeting as is_retargeting,
                AdvertisingID as advertising_id,
                IDFA as idfa,
                SiteID as site_id,
                Channel as channel,
                CB1MediaSource as CB1_media_source,
                CB1Campaign as CB1_campaign,
                CB2MediaSource as CB2_media_source,
                CB2Campaign as CB2_campaign,
                CB3MediaSource as CB3_media_source,
                CB3Campaign as CB3_campaign,
                TIMESTAMP(installTimeStamp) as install_time,
                TIMESTAMP(installTimeStamp) as event_time,
                'install' as event_type,
                TrackerAccountInstallDateKST as install_datekey
                FROM dataplatform-reporting.DataService.V_0271_0001_TrackerAccountInstallFirstFix_V
                WHERE TrackerAccountInstallDateKST = '{current_date_obj.strftime("%Y-%m-%d")}'
        ) as source 
        ON target.app_id = source.app_id
        AND target.joyple_game_code = source.joyple_game_code
        AND target.market_id = source.market_id
        AND target.tracker_account_id = source.tracker_account_id
        AND target.tracker_type_id = source.tracker_type_id
        WHEN NOT MATCHED BY target THEN
        INSERT (
            app_id,
            joyple_game_code,
            market_id,
            tracker_account_id,
            tracker_type_id,
            bundle_id,
            platform,
            country_code,
            media_source,
            media_source_cat,
            is_organic,
            agency,
            campaign,
            init_campaign,
            adset_name,
            ad_name,
            is_retargeting,
            advertising_id,
            idfa,
            site_id,
            channel,
            CB1_media_source,
            CB1_campaign,
            CB2_media_source,
            CB2_campaign,
            CB3_media_source,
            CB3_campaign,
            install_time,
            event_time,
            event_type,
            install_datekey
            )
            VALUES 
            (
                source.app_id,
                source.joyple_game_code,
                source.market_id,
                source.tracker_account_id,
                source.tracker_type_id,
                source.bundle_id,
                source.platform,
                source.country_code,
                source.media_source,
                source.media_source_cat,
                source.is_organic,
                source.agency,
                source.campaign,
                source.init_campaign,
                source.adset_name,
                source.ad_name,
                source.is_retargeting,
                source.advertising_id,
                source.idfa,
                source.site_id,
                source.channel,
                source.CB1_media_source,
                source.CB1_campaign,
                source.CB2_media_source,
                source.CB2_campaign,
                source.CB3_media_source,
                source.CB3_campaign,
                source.install_time,
                source.event_time,
                source.event_type,
                source.install_datekey
            )
        WHEN MATCHED THEN
        UPDATE SET
            target.install_time = source.install_time
            , target.event_time = source.event_time
            , target.event_type = source.event_type
            , target.install_datekey = source.install_datekey
                
        """
        # ─────────────────────────────────────────────────────────────────────

        query_job = client.query(query)

        try:
            print(f"📊 처리된 행 개수(Insert/Update): {query_job.num_dml_affected_rows}")
            print(f"■ {td_str} f_tracker_install Batch 완료")
        except Exception as e:
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            raise e

    print("✅ f_tracker_install ETL 완료")
    return True


# ── Task 래퍼 ─────────────────────────────────────────────────────────────────
def run_test(**context):
    logger = logging.getLogger(__name__)
    client = init_bq_client()

    # 원본 ETL과 동일하게 실행 시점(logical_date) 기준 어제 날짜 계산
    utc = pytz.utc
    kst = pytz.timezone('Asia/Seoul')
    run_date = context.get('logical_date') or context.get('execution_date') or datetime.now()
    run_date_kst = run_date.replace(tzinfo=utc).astimezone(kst)
    target_d = run_date_kst.date() - timedelta(days=1)
    target_date = [target_d.strftime("%Y-%m-%d")]
    print(f"▶ target_date: {target_date}")

    try:
        etl_f_tracker_install(target_date=target_date, client=client)
        logger.info("✅ 테스트 완료")
    except Exception as e:
        logger.error(f"❌ 테스트 실패: {e}")
        raise


# ── DAG 정의 ──────────────────────────────────────────────────────────────────
with DAG(
    dag_id="test_etl_f_tracker_install",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # 수동 트리거 전용
    catchup=False,
    tags=["test"],
) as dag:

    PythonOperator(
        task_id="run_etl_f_tracker_install",
        python_callable=run_test,
    )
