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


# ── 테스트 대상 함수 2 ──────────────────────────────────────────────────────────
def etl_f_tracker_first(target_date: list, client):

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
        MERGE `datahub-478802.datahub.f_tracker_first` as target
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
                TIMESTAMP(installTimeStamp) as first_tracking_datetime,
                TrackerAccountInstallDateKST as first_tracking_datekey
                FROM dataplatform-reporting.DataService.T_0273_0000_TrackerAccountFirst_V
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
            first_tracking_datetime,
            first_tracking_datekey
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
                source.first_tracking_datetime,
                source.first_tracking_datekey
            )
        WHEN MATCHED THEN
        UPDATE SET
              target.first_tracking_datetime = source.first_tracking_datetime
            , target.first_tracking_datekey = source.first_tracking_datekey
        """
        # ─────────────────────────────────────────────────────────────────────

        query_job = client.query(query)

        try:
            print(f"📊 처리된 행 개수(Insert/Update): {query_job.num_dml_affected_rows}")
            print(f"■ {td_str} f_tracker_first Batch 완료")
        except Exception as e:
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            raise e

    print("✅ f_tracker_first ETL 완료")
    return True


# ── 테스트 대상 함수 3 : etl_f_common_register ────────────────────────────────
def etl_f_common_register(target_date: list, client):

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

        query = f"""
        MERGE `datahub-478802.datahub.f_common_register_test` AS target
        USING
        (
            with TA as (
                SELECT a.GameID
                    , a.WorldID
                    , a.ServerName
                    , a.JoypleGameID
                    , a.AuthMethodID
                    , a.AuthAccountName
                    , a.GameAccountName
                    , a.GameSubUserName
                    , a.DeviceAccountName
                    , a.TrackerAccountName
                    , a.TrackerTypeID
                    , a.MarketID
                    , a.OSID
                    , a.PlatformDeviceType
                    , a.AccountLevel
                    , a.IP
                    , a.AccessTypeID
                    , a.PlaySeconds
                    , a.LogTime
                FROM `dataplatform-reporting.DataService.V_0160_0000_AccessLog_V` AS a
                WHERE a.LogTime >= '{start_utc}'
                AND a.LogTime < '{end_utc}'
                AND a.AccessTypeID = 1
            )
            , TB as (
                SELECT
                    DATE(Info.LogTime, "Asia/Seoul")      AS reg_datekey
                    , DATETIME(Info.LogTime, "Asia/Seoul")  AS reg_datetime
                    , Info.GameID               AS game_id
                    , Info.WorldID              AS world_id
                    , JoypleGameID              AS joyple_game_code
                    , Info.AuthMethodID         AS auth_method_id
                    , AuthAccountName           AS auth_account_name
                    , Info.TrackerAccountName   AS tracker_account_id
                    , Info.TrackerTypeID        AS tracker_type_id
                    , Info.DeviceAccountName    AS device_id
                    , b.country_code            AS country_code
                    , Info.MarketID             AS market_id
                    , Info.OSID                 AS os_id
                    , Info.PlatformDeviceType   AS platform_device_type
                FROM (
                    SELECT a.JoypleGameID
                        , a.AuthAccountName
                        , ARRAY_AGG(STRUCT(a.GameID, a.AuthMethodID, a.WorldID, a.TrackerTypeID, a.MarketID, a.OSID, a.IP, a.PlatformDeviceType, a.TrackerAccountName, a.DeviceAccountName, a.LogTime) ORDER BY a.LogTime ASC LIMIT 1)[OFFSET(0)] AS Info
                    FROM (
                        SELECT
                            a.GameID , a.WorldID , a.JoypleGameID , a.AuthMethodID , a.AuthAccountName
                            , a.TrackerAccountName , a.TrackerTypeID , a.DeviceAccountName , a.PlatformDeviceType
                            , a.MarketID , a.OSID , a.IP , a.LogTime
                        FROM TA AS a
                        WHERE a.AccessTypeID = 1
                        AND a.GameID IS NOT NULL
                        AND a.AuthAccountName IS NOT NULL
                    ) AS a
                    GROUP BY a.JoypleGameID, a.AuthAccountName
                ) AS a
                LEFT OUTER JOIN datahub-478802.datahub.dim_ip4_country_code AS b
                ON (a.Info.IP = b.ip)
            )
            , TC as (
                SELECT
                TB.reg_datekey,
                TB.reg_datetime,
                TB.game_id,
                TB.world_id,
                TB.joyple_game_code,
                TB.auth_method_id,
                TB.auth_account_name,
                TB.tracker_account_id,
                TB.tracker_type_id,
                TB.device_id,
                TB.country_code as reg_country_code,
                TB.market_id,
                TB.os_id,
                TB.platform_device_type,
                aa.app_id,
                aa.bundle_id,
                aa.country_code as first_tracking_country_code,
                aa.media_source,
                aa.media_source_cat,
                if(J.auth_account_name is not null, 'Non-Organic',aa.is_organic) as is_organic,
                aa.agency,
                coalesce(J.Campaign_name,aa.campaign)                        as campaign,
                coalesce(J.Campaign_name,aa.init_campaign)                   as init_campaign,
                aa.adset_name,
                coalesce(cast(J.ad_name as string),aa.ad_name)               as ad_name,
                aa.is_retargeting,
                aa.advertising_id,
                aa.idfa,
                aa.site_id,
                aa.channel,
                aa.CB1_media_source,
                aa.CB1_campaign,
                aa.CB2_media_source,
                aa.CB2_campaign,
                aa.CB3_media_source,
                aa.CB3_campaign,
                aa.first_tracking_datetime,
                aa.first_tracking_datekey
                FROM TB
                LEFT JOIN datahub-478802.datahub.f_tracker_first as aa
                ON TB.tracker_account_id = aa.tracker_account_id AND TB.tracker_type_id = aa.tracker_type_id
                LEFT JOIN (
                    SELECT a.*, b.* except(campaign_name, joyple_game_code)
                    FROM `datahub-478802.datahub.pre_joytracking_tracker` as a
                    LEFT JOIN `datahub-478802.datahub.dim_pccampaign_list_joytracking` as b
                    ON a.campaign_name = b.campaign_name AND a.joyple_game_code = b.joyple_game_code) as J
                ON TB.joyple_game_code = j.joyple_game_code AND TB.auth_account_name = j.auth_account_name
            )
            SELECT * FROM TC
            QUALIFY ROW_NUMBER() OVER(
                PARTITION BY joyple_game_code, auth_account_name
                ORDER BY first_tracking_datetime DESC, reg_datetime DESC
            ) = 1
        ) AS source ON target.joyple_game_code = source.joyple_game_code AND CAST(target.auth_account_name AS STRING) = CAST(source.auth_account_name AS STRING)

        WHEN MATCHED AND (target.campaign <> source.campaign OR target.media_source <> source.media_source)
                    AND source.app_id IS NOT NULL THEN
        UPDATE SET
            target.app_id = source.app_id
            , target.bundle_id = source.bundle_id
            , target.first_tracking_country_code = source.first_tracking_country_code
            , target.media_source = source.media_source
            , target.media_source_cat = source.media_source_cat
            , target.is_organic = source.is_organic
            , target.agency = source.agency
            , target.campaign = source.campaign
            , target.init_campaign = source.init_campaign
            , target.adset_name = source.adset_name
            , target.ad_name = source.ad_name
            , target.is_retargeting = source.is_retargeting
            , target.advertising_id = source.advertising_id
            , target.idfa = source.idfa
            , target.site_id = source.site_id
            , target.channel = source.channel
            , target.CB1_media_source = source.CB1_media_source
            , target.CB1_campaign = source.CB1_campaign
            , target.CB2_media_source = source.CB2_media_source
            , target.CB2_campaign = source.CB2_campaign
            , target.CB3_media_source = source.CB3_media_source
            , target.CB3_campaign = source.CB3_campaign
            , target.first_tracking_datetime = source.first_tracking_datetime
            , target.first_tracking_datekey = source.first_tracking_datekey

        WHEN NOT MATCHED BY target THEN
        INSERT(
            reg_datekey, reg_datetime, game_id, world_id, joyple_game_code, auth_method_id,
            auth_account_name, tracker_account_id, tracker_type_id, device_id, reg_country_code,
            market_id, os_id, platform_device_type, app_id, bundle_id, first_tracking_country_code,
            media_source, media_source_cat, is_organic, agency, campaign, init_campaign,
            adset_name, ad_name, is_retargeting, advertising_id, idfa, site_id, channel,
            CB1_media_source, CB1_campaign, CB2_media_source, CB2_campaign, CB3_media_source,
            CB3_campaign, first_tracking_datetime, first_tracking_datekey
            )
        VALUES(
            source.reg_datekey, source.reg_datetime, source.game_id, source.world_id,
            source.joyple_game_code, source.auth_method_id, source.auth_account_name,
            source.tracker_account_id, source.tracker_type_id, source.device_id,
            source.reg_country_code, source.market_id, source.os_id, source.platform_device_type,
            source.app_id, source.bundle_id, source.first_tracking_country_code, source.media_source,
            source.media_source_cat, source.is_organic, source.agency, source.campaign,
            source.init_campaign, source.adset_name, source.ad_name, source.is_retargeting,
            source.advertising_id, source.idfa, source.site_id, source.channel,
            source.CB1_media_source, source.CB1_campaign, source.CB2_media_source,
            source.CB2_campaign, source.CB3_media_source, source.CB3_campaign,
            source.first_tracking_datetime, source.first_tracking_datekey
            );
        """

        query_job = client.query(query)
        try:
            query_job.result()
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} f_common_register Batch 완료")
        except Exception as e:
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            raise e

    print("✅ f_common_register ETL 완료")
    return True


# ── 테스트 대상 함수 4 : adjust_f_common_register ─────────────────────────────
def adjust_f_common_register(target_date: list, client):

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

        query = f"""
        MERGE datahub-478802.datahub.f_common_register_test AS target
        USING
        (
            select DATE(TA.INFO.log_time, "Asia/Seoul") as reg_datekey
            , DATETIME(TA.INFO.log_time, "Asia/Seoul") as reg_datetime
            , TA.INFO.game_id
            , TA.INFO.world_id
            , TA.joyple_game_code
            , TA.INFO.auth_method_id
            , TA.auth_account_name
            , TA.INFO.tracker_account_id
            , TA.INFO.mmp_type as tracker_type_id
            , TA.INFO.device_id
            , TC.country_code as reg_country_code
            , TA.INFO.market_id
            , TA.INFO.os_id
            , TA.INFO.platform_device_type
            , TA.INFO.app_id
            , TB.bundle_id
            , TC.country_code as first_tracking_country_code
            , TB.media_source
            , TB.media_source_cat
            , if(J.auth_account_name is not null, 'Non-Organic',TB.is_organic) as is_organic
            , TB.agency
            , coalesce(J.Campaign_name,TB.campaign) as campaign
            , coalesce(J.Campaign_name,TB.init_campaign) as init_campaign
            , TB.adset_name
            , coalesce(cast(J.ad_name as string),TB.ad_name) as ad_name
            , TB.is_retargeting
            , TB.advertising_id
            , TB.idfa
            , TB.site_id
            , TB.channel
            , TB.CB1_media_source
            , TB.CB1_campaign
            , TB.CB2_media_source
            , TB.CB2_campaign
            , TB.CB3_media_source
            , TB.CB3_campaign
            , TB.first_tracking_datetime
            , TB.first_tracking_datekey
            from
            (
                select JoypleGameID as joyple_game_code
                     , AuthAccountName as auth_account_name
                     , array_agg(STRUCT(gameid as game_id,
                                        worldid as world_id,
                                        AuthMethodID as auth_method_id,
                                        AuthAccountName as auth_account_name,
                                        TrackerAccountName as tracker_account_id,
                                        TrackerTypeID as mmp_type,
                                        DeviceID as device_id,
                                        IP as ip,
                                        MarketID as market_id,
                                        OSID as os_id,
                                        PlatformDeviceType as platform_device_type,
                                        AppID as app_id,
                                        LogTime as log_time) ORDER BY logtime asc)[OFFSET(0)] AS INFO
                from `dataplatform-reporting.DataService.V_0156_0000_CommonLogPaymentFix_V`
                where LogTime >= '{start_utc}'
                and LogTime < '{end_utc}'
                group by joyple_game_code, auth_account_name
            ) TA
            left join
            datahub-478802.datahub.f_tracker_first as TB
            on TA.INFO.tracker_account_id = TB.tracker_account_id AND TA.INFO.mmp_type = TB.tracker_type_id
            left join
            datahub-478802.datahub.dim_ip4_country_code as TC
            on TA.INFO.ip = TC.ip
            LEFT JOIN (
                       SELECT a.*, b.* except(campaign_name, joyple_game_code)
                       FROM `datahub-478802.datahub.pre_joytracking_tracker` as a
                       LEFT JOIN `datahub-478802.datahub.dim_pccampaign_list_joytracking` as b
                       ON a.campaign_name = b.campaign_name AND a.joyple_game_code = b.joyple_game_code) as J
            ON TA.joyple_game_code = j.joyple_game_code AND TA.auth_account_name = j.auth_account_name
        ) AS source ON target.joyple_game_code = source.joyple_game_code AND target.auth_account_name = source.auth_account_name
        WHEN NOT MATCHED BY target THEN
        INSERT
        (
            reg_datekey, reg_datetime, game_id, world_id, joyple_game_code, auth_method_id,
            auth_account_name, tracker_account_id, tracker_type_id, device_id, reg_country_code,
            market_id, os_id, platform_device_type, app_id, bundle_id, first_tracking_country_code,
            media_source, media_source_cat, is_organic, agency, campaign, init_campaign,
            adset_name, ad_name, is_retargeting, advertising_id, idfa, site_id, channel,
            CB1_media_source, CB1_campaign, CB2_media_source, CB2_campaign, CB3_media_source,
            CB3_campaign, first_tracking_datetime, first_tracking_datekey
        )
        VALUES
        (
            source.reg_datekey, source.reg_datetime, source.game_id, source.world_id,
            source.joyple_game_code, source.auth_method_id, source.auth_account_name,
            source.tracker_account_id, source.tracker_type_id, source.device_id,
            source.reg_country_code, source.market_id, source.os_id, source.platform_device_type,
            source.app_id, source.bundle_id, source.first_tracking_country_code, source.media_source,
            source.media_source_cat, source.is_organic, source.agency, source.campaign,
            source.init_campaign, source.adset_name, source.ad_name, source.is_retargeting,
            source.advertising_id, source.idfa, source.site_id, source.channel,
            source.CB1_media_source, source.CB1_campaign, source.CB2_media_source,
            source.CB2_campaign, source.CB3_media_source, source.CB3_campaign,
            source.first_tracking_datetime, source.first_tracking_datekey
        )
        WHEN MATCHED AND (target.reg_datekey > source.reg_datekey)
        THEN
        UPDATE SET
        target.reg_datekey = source.reg_datekey;
        """

        query_job = client.query(query)
        try:
            query_job.result()
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} f_common_register_adjust Batch 완료")
        except Exception as e:
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            raise e

    print("✅ f_common_register_adjust ETL 완료")
    return True


# ── 테스트 대상 함수 5 : etl_f_user_map ──────────────────────────────────────
def etl_f_user_map(target_date: list, client):

    for td in target_date:
        start_date = datetime.strptime(td, "%Y-%m-%d")
        end_date = start_date + timedelta(days=1)
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")
        print(f"📝 시작시간 : {start_date}  📝 종료시간 : {end_date}")
        print(f"■ {start_date} f_user_map Batch 시작")

        query = f"""
        MERGE datahub-478802.datahub.f_user_map_test as target
        USING
        ( 
                select A.datekey
                , A.joyple_game_code
                , A.auth_method_id
                , A.auth_account_name
                , A.RU
                , B.reg_datekey
                , B.reg_datetime
                , DATE_DIFF(A.datekey, B.reg_datekey, DAY) as datediff_reg
                , B.reg_country_code
                , B.market_id as reg_market_id
                , B.os_id as reg_os_id
                , B.platform_device_type as reg_platform_device_type
                , B.first_tracking_datekey
                , DATE_DIFF(A.datekey, B.first_tracking_datekey, DAY) as datediff_first_tracking
                , B.tracker_account_id
                , B.tracker_type_id
                , B.app_id
                , B.media_source
                , B.bundle_id
                , B.is_organic
                , B.agency
                , B.campaign
                , B.init_campaign
                , B.adset_name
                , B.ad_name
                , B.is_retargeting
                , B.advertising_id
                , B.idfa
                , B.site_id
                , B.channel
                , B.CB1_media_source
                , B.CB1_campaign
                , B.CB2_media_source
                , B.CB2_campaign
                , B.CB3_media_source
                , B.CB3_campaign
                , C.daily_total_rev
                , C.daily_buy_cnt
                , C.daily_pu
                , C.stacked_rev
                , C.stacked_buy_cnt
                , C.stacked_rev_percent_rank
                , C.monthly_rev
                , CASE WHEN C.monthly_rev > 10000000 then '1.R0'
                        WHEN C.monthly_rev > 1000000 then '2.R1'
                        WHEN C.monthly_rev > 100000 then '3.R2'
                        WHEN C.monthly_rev > 10000 then '4.R3'
                        WHEN C.monthly_rev > 0 then '5.R4'
                        ELSE '6.Non_pu' END AS monthly_rgroup
                , C.before_monthly_rev
                , CASE WHEN C.before_monthly_rev > 10000000 then '1.R0'
                        WHEN C.before_monthly_rev > 1000000 then '2.R1'
                        WHEN C.before_monthly_rev > 100000 then '3.R2'
                        WHEN C.before_monthly_rev > 10000 then '4.R3'
                        WHEN C.before_monthly_rev > 0 then '5.R4'
                        ELSE '6.Non_pu' END AS before_monthly_rgroup
                , C.yearly_rev
                , C.last_30days_rev
                , C.first_payment_datekey
                , C.last_payment_datekey
                , E.stacked_IAA_watch_cnt
                , E.stacked_IAA_rev
                , E.daily_IAA_watch_cnt
                , E.daily_IAA_rev
                , E.monthly_IAA_rev
                , F.play_seconds
                , F.access_cnt
                , F.daily_game_user_level
                , G.last_login_datekey
                , G.max_game_user_level
                , DATE_DIFF(A.datekey, G.last_login_datekey, DAY) as datediff_last_login
                , F.stickiness
                , IF(C.daily_pu = 1 AND A.RU = 1, 1, 0) as NRPU
                , H.game_sub_user_name_cnt
                , I.products_array
                from
                (
                    SELECT datekey, joyple_game_code, auth_account_name, MIN(auth_method_id) as auth_method_id
                    , MAX(IF(datekey = reg_datekey, 1, 0)) AS RU
                    FROM datahub-478802.datahub.f_common_access
                    where datekey >= '{start_date}' and datekey < '{end_date}'
                    and access_type_id = 1
                    group by 1,2,3
                ) AS A
                LEFT OUTER JOIN
                (
                    SELECT joyple_game_code, auth_account_name,
                    reg_datekey, reg_datetime,
                    tracker_account_id, tracker_type_id,
                    reg_country_code, market_id, os_id, platform_device_type,
                    install_country_code, first_tracking_datekey,
                    app_id, media_source, bundle_id, is_organic, agency, campaign, init_campaign,
                    adset_name, ad_name, is_retargeting, advertising_id, idfa, site_id,
                    channel, CB1_media_source, CB1_campaign, CB2_media_source, CB2_campaign, CB3_media_source, CB3_campaign
                    FROM datahub-478802.datahub.f_common_register_test
                    WHERE reg_datekey < '{end_date}'
                ) AS B
                ON A.joyple_game_code = B.joyple_game_code AND A.auth_account_name = B.auth_account_name
                LEFT OUTER JOIN
                (
                    SELECT joyple_game_code, auth_account_name,
                    SUM(IF(datekey >= '{start_date}' AND datekey < '{end_date}', revenue, 0)) as daily_total_rev,
                    SUM(IF(datekey >= '{start_date}' AND datekey < '{end_date}', buy_cnt, 0)) as daily_buy_cnt,
                    MAX(IF(datekey >= '{start_date}' AND datekey < '{end_date}', 1, 0)) as daily_pu,
                    SUM(revenue) as stacked_rev,
                    sum(buy_cnt) as stacked_buy_cnt,
                    PERCENT_RANK() OVER (PARTITION BY joyple_game_code ORDER BY sum(revenue) DESC) AS stacked_rev_percent_rank,
                    SUM(IF(datekey >= DATE_TRUNC(DATE('{start_date}'), MONTH) AND datekey <= LAST_DAY(DATE_TRUNC(DATE('{start_date}'), MONTH), MONTH), revenue, 0)) as monthly_rev,
                    SUM(IF(datekey >= DATE_TRUNC(DATE_SUB(DATE('{start_date}'), INTERVAL 1 MONTH), MONTH) AND datekey <= LAST_DAY(DATE_TRUNC(DATE_SUB(DATE('{start_date}'), INTERVAL 1 MONTH), MONTH), MONTH), revenue, 0)) as before_monthly_rev,
                    SUM(IF(datekey >= DATE_TRUNC(DATE('{start_date}'), YEAR) AND datekey <= LAST_DAY(DATE_TRUNC(DATE('{start_date}'), YEAR), YEAR), revenue, 0)) as yearly_rev,
                    SUM(IF(datekey >= DATE_SUB('{start_date}', INTERVAL 30 DAY) AND datekey < '{end_date}', revenue, 0)) as last_30days_rev,
                    min(datekey) as first_payment_datekey,
                    MAX(IF(datekey < '{start_date}', datekey, null)) as last_payment_datekey
                    FROM datahub-478802.datahub.f_common_payment
                    WHERE datekey < '{end_date}'
                    group by joyple_game_code, auth_account_name
                ) AS C
                ON A.joyple_game_code = C.joyple_game_code AND A.auth_account_name = C.auth_account_name
                LEFT OUTER JOIN
                (
                    SELECT joyple_game_code, auth_account_name,
                    sum(watch_cnt) as stacked_IAA_watch_cnt,
                    sum(revenue_per_user_krw) as stacked_IAA_rev,
                    sum(IF(watch_datekey >= '{start_date}' AND watch_datekey < '{end_date}', watch_cnt, 0)) as daily_IAA_watch_cnt,
                    sum(IF(watch_datekey >= '{start_date}' AND watch_datekey < '{end_date}', revenue_per_user_krw, 0)) as daily_IAA_rev,
                    SUM(IF(watch_datekey >= DATE_TRUNC(DATE('{start_date}'), MONTH) AND watch_datekey <= LAST_DAY(DATE_TRUNC(DATE('{start_date}'), MONTH), MONTH), revenue_per_user_krw, 0)) as monthly_IAA_rev
                    FROM datahub-478802.datahub.f_IAA_auth_account_performance
                    where watch_datekey < '{end_date}'
                    group by joyple_game_code, auth_account_name
                ) AS E
                ON A.joyple_game_code = E.joyple_game_code AND A.auth_account_name = E.auth_account_name
                LEFT OUTER JOIN
                (
                    SELECT joyple_game_code, auth_account_name
                    , SUM(IF(datekey >= '{start_date}' AND datekey < '{end_date}', play_seconds, 0)) as play_seconds
                    , SUM(IF(datekey >= '{start_date}' AND datekey < '{end_date}', access_cnt, 0)) as access_cnt
                    , MAX(IF(datekey >= '{start_date}' AND datekey < '{end_date}', game_user_level, null)) as daily_game_user_level
                    , COUNT(DISTINCT(IF(datekey >= DATE_SUB('{start_date}', INTERVAL 6 DAY) AND datekey < '{end_date}' AND access_type_id = 1, datekey, null))) as stickiness
                    FROM datahub-478802.datahub.f_common_access
                    where datekey >= DATE_SUB('{start_date}', INTERVAL 6 DAY) AND datekey < '{end_date}'
                    group by joyple_game_code, auth_account_name
                ) AS F
                ON A.joyple_game_code = F.joyple_game_code AND A.auth_account_name = F.auth_account_name
                LEFT OUTER JOIN
                (
                    SELECT joyple_game_code, auth_account_name
                        , MAX(last_login_datekey) as last_login_datekey
                        , MAX(max_game_user_level) as max_game_user_level
                    FROM datahub-478802.datahub.f_common_access_last_login
                    group by joyple_game_code, auth_account_name
                ) AS G
                ON A.joyple_game_code = G.joyple_game_code AND A.auth_account_name = G.auth_account_name
                LEFT OUTER JOIN
                (
                    SELECT joyple_game_code, auth_account_name
                        , count(DISTINCT game_sub_user_name) as game_sub_user_name_cnt
                    FROM `datahub-478802.datahub.f_common_register_char`
                    where reg_datekey < '{end_date}'
                    GROUP BY joyple_game_code, auth_account_name
                ) AS H
                ON A.joyple_game_code = H.joyple_game_code AND A.auth_account_name = H.auth_account_name
                LEFT OUTER JOIN
                (
                    SELECT joyple_game_code, auth_account_name,
                        ARRAY_AGG(STRUCT(pg_id, platform_device_type, product_code, product_name, revenue, buy_cnt) ORDER BY datekey ASC) as products_array
                    FROM `datahub-478802.datahub.f_common_payment`
                    WHERE datekey >= '{start_date}' and datekey < '{end_date}'
                    GROUP BY joyple_game_code, auth_account_name
                ) AS I
                ON A.joyple_game_code = I.joyple_game_code AND A.auth_account_name  = I.auth_account_name
        ) as source
        ON target.datekey = source.datekey
        AND target.joyple_game_code = source.joyple_game_code
        AND target.auth_account_name = source.auth_account_name
        WHEN NOT MATCHED BY target THEN
        INSERT
        (
        datekey, joyple_game_code, auth_method_id, auth_account_name, RU,
        reg_datekey, reg_datetime, datediff_reg, reg_country_code, reg_market_id,
        reg_os_id, reg_platform_device_type, first_tracking_datekey, datediff_first_tracking,
        tracker_account_id, tracker_type_id, app_id, media_source, bundle_id,
        is_organic, agency, campaign, init_campaign, adset_name, ad_name,
        is_retargeting, advertising_id, idfa, site_id, channel,
        CB1_media_source, CB1_campaign, CB2_media_source, CB2_campaign,
        CB3_media_source, CB3_campaign, daily_total_rev, daily_buy_cnt, daily_pu,
        stacked_rev, stacked_buy_cnt, stacked_rev_percent_rank,
        monthly_rev, monthly_rgroup, before_monthly_rev, before_monthly_rgroup,
        yearly_rev, last_30days_rev, first_payment_datekey, last_payment_datekey,
        stacked_IAA_watch_cnt, stacked_IAA_rev, daily_IAA_watch_cnt, daily_IAA_rev,
        monthly_IAA_rev, play_seconds, access_cnt, daily_game_user_level,
        last_login_datekey, max_game_user_level, datediff_last_login,
        stickiness, NRPU, game_sub_user_name_cnt, products_array
        )
        VALUES
        (
          source.datekey, source.joyple_game_code, source.auth_method_id,
          source.auth_account_name, source.RU, source.reg_datekey, source.reg_datetime,
          source.datediff_reg, source.reg_country_code, source.reg_market_id,
          source.reg_os_id, source.reg_platform_device_type, source.first_tracking_datekey,
          source.datediff_first_tracking, source.tracker_account_id, source.tracker_type_id,
          source.app_id, source.media_source, source.bundle_id, source.is_organic,
          source.agency, source.campaign, source.init_campaign, source.adset_name,
          source.ad_name, source.is_retargeting, source.advertising_id, source.idfa,
          source.site_id, source.channel, source.CB1_media_source, source.CB1_campaign,
          source.CB2_media_source, source.CB2_campaign, source.CB3_media_source,
          source.CB3_campaign, source.daily_total_rev, source.daily_buy_cnt,
          source.daily_pu, source.stacked_rev, source.stacked_buy_cnt,
          source.stacked_rev_percent_rank, source.monthly_rev, source.monthly_rgroup,
          source.before_monthly_rev, source.before_monthly_rgroup, source.yearly_rev,
          source.last_30days_rev, source.first_payment_datekey, source.last_payment_datekey,
          source.stacked_IAA_watch_cnt, source.stacked_IAA_rev, source.daily_IAA_watch_cnt,
          source.daily_IAA_rev, source.monthly_IAA_rev, source.play_seconds,
          source.access_cnt, source.daily_game_user_level, source.last_login_datekey,
          source.max_game_user_level, source.datediff_last_login, source.stickiness,
          source.NRPU, source.game_sub_user_name_cnt, source.products_array
        )
        ;
        """
        client.query(query)
        print(f"■ {start_date} f_user_map Batch 완료")

    print("✅ f_user_map ETL 완료")
    return True


# ── 테스트 대상 함수 6 : etl_f_user_map_char ─────────────────────────────────
def etl_f_user_map_char(target_date: list, client):

    for td in target_date:
        start_date = datetime.strptime(td, "%Y-%m-%d")
        end_date = start_date + timedelta(days=1)
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")
        print(f"📝 시작시간 : {start_date}  📝 종료시간 : {end_date}")

        query = f"""
        MERGE datahub-478802.datahub.f_user_map_char_test as target
        USING
        (
            SELECT A.datekey, A.joyple_game_code, A.auth_account_name, A.auth_method_id,
            A.game_sub_user_name, A.RU, B.game_id, B.world_id, B.server_name,
            B.country_code, B.market_id, B.os_id, B.platform_device_type,
            B.tracker_type_id, B.tracker_account_id, B.game_sub_user_reg_datekey,
            DATE_DIFF(A.datekey, B.game_sub_user_reg_datekey, DAY) as datediff_sub_user_reg,
            B.game_sub_user_reg_datetime, B.reg_datekey,
            DATE_DIFF(A.datekey, B.reg_datekey, DAY) as datediff_reg,
            C.daily_total_rev, C.daily_buy_cnt, C.daily_pu, C.stacked_rev,
            C.stacked_buy_cnt, C.stacked_rev_percent_rank, C.monthly_rev,
            CASE WHEN C.monthly_rev > 10000000 then '1.R0' WHEN C.monthly_rev > 1000000 then '2.R1'
                    WHEN C.monthly_rev > 100000 then '3.R2' WHEN C.monthly_rev > 10000 then '4.R3'
                    WHEN C.monthly_rev > 0 then '5.R4' ELSE '6.Non_pu' END as monthly_rgroup_sub_user,
            C.before_monthly_rev,
            CASE WHEN C.before_monthly_rev > 10000000 then '1.R0' WHEN C.before_monthly_rev > 1000000 then '2.R1'
                    WHEN C.before_monthly_rev > 100000 then '3.R2' WHEN C.before_monthly_rev > 10000 then '4.R3'
                    WHEN C.before_monthly_rev > 0 then '5.R4' ELSE '6.Non_pu' END as before_monthly_rgroup_sub_user,
            C.yearly_rev, C.last_30days_rev, C.first_payment_datekey, C.last_payment_datekey,
            D.stacked_IAA_rev, D.daily_IAA_rev, E.play_seconds, E.access_cnt,
            H.max_game_user_level, H.last_login_datekey,
            DATE_DIFF(A.datekey, H.last_login_datekey, DAY) as datediff_last_login_sub_user,
            E.stickiness, F.products_array, G.first_tracking_country_code, G.first_tracking_datekey,
            G.app_id, G.media_source, G.bundle_id, G.is_organic, G.agency, G.campaign,
            G.init_campaign, G.adset_name, G.ad_name, G.is_retargeting, G.advertising_id,
            G.idfa, G.site_id, G.channel, G.CB1_media_source, G.CB1_campaign,
            G.CB2_media_source, G.CB2_campaign, G.CB3_media_source, G.CB3_campaign
                FROM (
                    SELECT datekey, joyple_game_code, auth_account_name, game_sub_user_name, MIN(auth_method_id) as auth_method_id,
                        MAX(IF(datekey = reg_datekey, 1, 0)) AS RU
                    FROM `datahub-478802.datahub.f_common_access`
                    WHERE datekey >= '{start_date}' and datekey < '{end_date}' AND access_type_id = 1
                    GROUP BY 1,2,3,4
                ) AS A
                LEFT OUTER JOIN
                    (SELECT * FROM `datahub-478802.datahub.f_common_register_char` WHERE game_sub_user_reg_datekey < '{end_date}') AS B
                ON A.joyple_game_code = B.joyple_game_code AND A.auth_account_name = B.auth_account_name AND COALESCE(A.game_sub_user_name, 'dafault') = COALESCE(B.game_sub_user_name, 'dafault')
                LEFT OUTER JOIN (
                    SELECT joyple_game_code, auth_account_name, game_sub_user_name,
                        SUM(IF(datekey >= '{start_date}' AND datekey < '{end_date}', revenue, 0)) as daily_total_rev,
                        SUM(IF(datekey >= '{start_date}' AND datekey < '{end_date}', buy_cnt, 0)) as daily_buy_cnt,
                        MAX(IF(datekey >= '{start_date}' AND datekey < '{end_date}', 1, 0)) as daily_pu,
                        SUM(revenue) as stacked_rev, sum(buy_cnt) as stacked_buy_cnt,
                        PERCENT_RANK() OVER (PARTITION BY joyple_game_code ORDER BY sum(revenue) DESC) AS stacked_rev_percent_rank,
                        SUM(IF(datekey >= DATE_TRUNC(DATE('{start_date}'), MONTH) AND datekey <= LAST_DAY(DATE_TRUNC(DATE('{start_date}'), MONTH), MONTH), revenue, 0)) as monthly_rev,
                        SUM(IF(datekey >= DATE_TRUNC(DATE_SUB(DATE('{start_date}'), INTERVAL 1 MONTH), MONTH) AND datekey <= LAST_DAY(DATE_TRUNC(DATE_SUB(DATE('{start_date}'), INTERVAL 1 MONTH), MONTH), MONTH), revenue, 0)) as before_monthly_rev,
                        SUM(IF(datekey >= DATE_TRUNC(DATE('{start_date}'), YEAR) AND datekey <= LAST_DAY(DATE_TRUNC(DATE('{start_date}'), YEAR), YEAR), revenue, 0)) as yearly_rev,
                        SUM(IF(datekey >= DATE_SUB('{start_date}', INTERVAL 30 DAY) AND datekey < '{end_date}', revenue, 0)) as last_30days_rev,
                        min(datekey) as first_payment_datekey,
                        MAX(IF(datekey < '{start_date}', datekey, null)) as last_payment_datekey
                    FROM `datahub-478802.datahub.f_common_payment` WHERE datekey < '{end_date}'
                    GROUP BY 1,2,3
                ) AS C ON A.joyple_game_code = C.joyple_game_code AND A.auth_account_name = C.auth_account_name AND COALESCE(A.game_sub_user_name, 'dafault') = COALESCE(C.game_sub_user_name, 'dafault')
                LEFT OUTER JOIN (
                    SELECT joyple_game_code, auth_account_name, game_sub_user_name,
                            SUM(revenue_per_user_KRW) as stacked_IAA_rev,
                            SUM(IF(watch_datekey >= '{start_date}' AND watch_datekey < '{end_date}', revenue_per_user_KRW, 0)) as daily_IAA_rev
                    FROM `datahub-478802.datahub.f_IAA_auth_account_performance_joyple` WHERE watch_datekey < '{end_date}'
                    GROUP BY 1,2,3
                ) AS D ON A.joyple_game_code = D.joyple_game_code AND A.auth_account_name = D.auth_account_name AND COALESCE(A.game_sub_user_name, 'dafault') = COALESCE(D.game_sub_user_name, 'dafault')
                LEFT OUTER JOIN (
                    SELECT joyple_game_code, auth_account_name, game_sub_user_name,
                        SUM(IF(datekey >= '{start_date}' AND datekey < '{end_date}', play_seconds, 0)) as play_seconds,
                        SUM(IF(datekey >= '{start_date}' AND datekey < '{end_date}', access_cnt, 0)) as access_cnt,
                        MAX(game_user_level) as max_game_user_level,
                        MAX(IF(datekey < '{start_date}' AND access_type_id = 1, datekey, null)) as last_login_datekey,
                        COUNT(DISTINCT(IF(datekey >= DATE_SUB('{start_date}', INTERVAL 6 DAY) AND datekey < '{end_date}' AND access_type_id = 1, datekey, null))) as stickiness
                    FROM `datahub-478802.datahub.f_common_access` WHERE datekey >= DATE_SUB('{start_date}', INTERVAL 6 DAY) AND datekey < '{end_date}'
                    GROUP BY 1,2,3
                ) AS E ON A.joyple_game_code = E.joyple_game_code AND A.auth_account_name = E.auth_account_name AND COALESCE(A.game_sub_user_name, 'dafault') = COALESCE(E.game_sub_user_name, 'dafault')
                LEFT OUTER JOIN (
                    SELECT joyple_game_code, auth_account_name, game_sub_user_name,
                        ARRAY_AGG(STRUCT(pg_id, platform_device_type, product_code, product_name, revenue, buy_cnt) ORDER BY datekey ASC) as products_array
                    FROM `datahub-478802.datahub.f_common_payment` WHERE datekey >= '{start_date}' and datekey < '{end_date}'
                    GROUP BY 1,2,3
                ) AS F ON A.joyple_game_code = F.joyple_game_code AND A.auth_account_name = F.auth_account_name AND COALESCE(A.game_sub_user_name, 'dafault') = COALESCE(F.game_sub_user_name, 'dafault')
                LEFT OUTER JOIN (SELECT * FROM `datahub-478802.datahub.f_common_register_test` WHERE reg_datekey < '{end_date}') AS G
                    ON A.joyple_game_code = G.joyple_game_code AND A.auth_account_name = G.auth_account_name
                LEFT OUTER JOIN (
                    SELECT joyple_game_code, auth_account_name, game_sub_user_name,
                        MAX(last_login_datekey) as last_login_datekey, MAX(max_game_user_level) as max_game_user_level
                    FROM `datahub-478802.datahub.f_common_access_last_login` GROUP BY 1,2,3
                ) AS H ON A.joyple_game_code = H.joyple_game_code AND A.auth_account_name = H.auth_account_name AND COALESCE(A.game_sub_user_name, 'dafault') = COALESCE(H.game_sub_user_name, 'dafault')
        ) as source
        ON target.datekey = source.datekey
        AND target.joyple_game_code = source.joyple_game_code
        AND target.auth_account_name = source.auth_account_name
        AND target.game_sub_user_name = source.game_sub_user_name
        WHEN NOT MATCHED BY target THEN
        INSERT
        (
                    datekey, joyple_game_code, auth_account_name, auth_method_id,
                    game_sub_user_name, RU, game_id, world_id, server_name,
                    reg_country_code, reg_market_id, reg_os_id, platform_device_type,
                    tracker_type_id, tracker_account_id, game_sub_user_reg_datekey,
                    datediff_sub_user_reg, game_sub_user_reg_datetime, user_reg_datekey,
                    datediff_reg, daily_total_rev, daily_buy_cnt, daily_pu, stacked_rev,
                    stacked_buy_cnt, stacked_rev_percent_rank, monthly_rev,
                    monthly_rgroup_sub_user, before_monthly_rev, before_monthly_rgroup_sub_user,
                    yearly_rev, last_30days_rev, first_payment_datekey, last_payment_datekey,
                    stacked_IAA_rev, daily_IAA_rev, play_seconds, access_cnt,
                    max_game_user_level, last_login_datekey, datediff_last_login_sub_user,
                    stickiness, products_array, first_tracking_country_code, first_tracking_datekey,
                    app_id, media_source, bundle_id, is_organic, agency, campaign, init_campaign,
                    adset_name, ad_name, is_retargeting, advertising_id, idfa, site_id, channel,
                    CB1_media_source, CB1_campaign, CB2_media_source, CB2_campaign,
                    CB3_media_source, CB3_campaign
        )
        VALUES
        (
                    source.datekey, source.joyple_game_code, source.auth_account_name,
                    source.auth_method_id, source.game_sub_user_name, source.RU,
                    source.game_id, source.world_id, source.server_name,
                    source.country_code, source.market_id, source.os_id,
                    source.platform_device_type, source.tracker_type_id, source.tracker_account_id,
                    source.game_sub_user_reg_datekey, source.datediff_sub_user_reg,
                    source.game_sub_user_reg_datetime, source.reg_datekey, source.datediff_reg,
                    source.daily_total_rev, source.daily_buy_cnt, source.daily_pu,
                    source.stacked_rev, source.stacked_buy_cnt, source.stacked_rev_percent_rank,
                    source.monthly_rev, source.monthly_rgroup_sub_user, source.before_monthly_rev,
                    source.before_monthly_rgroup_sub_user, source.yearly_rev, source.last_30days_rev,
                    source.first_payment_datekey, source.last_payment_datekey,
                    source.stacked_IAA_rev, source.daily_IAA_rev, source.play_seconds,
                    source.access_cnt, source.max_game_user_level, source.last_login_datekey,
                    source.datediff_last_login_sub_user, source.stickiness, source.products_array,
                    source.first_tracking_country_code, source.first_tracking_datekey, source.app_id,
                    source.media_source, source.bundle_id, source.is_organic, source.agency,
                    source.campaign, source.init_campaign, source.adset_name, source.ad_name,
                    source.is_retargeting, source.advertising_id, source.idfa, source.site_id,
                    source.channel, source.CB1_media_source, source.CB1_campaign,
                    source.CB2_media_source, source.CB2_campaign, source.CB3_media_source,
                    source.CB3_campaign
        )
        """
        client.query(query)
        print(f"■ {start_date} f_user_map_char Batch 완료")

    print("✅ f_user_map_char ETL 완료")
    return True


# ── 날짜 계산 공통 함수 ───────────────────────────────────────────────────────
def _calc_target_date(context):
    utc = pytz.utc
    kst = pytz.timezone('Asia/Seoul')
    run_date = context.get('logical_date') or context.get('execution_date') or datetime.now()
    run_date_kst = run_date.replace(tzinfo=utc).astimezone(kst)
    target_d = run_date_kst.date() - timedelta(days=1)
    return [target_d.strftime("%Y-%m-%d")]


# ── Task 래퍼 ─────────────────────────────────────────────────────────────────
def run_test_install(**context):
    logger = logging.getLogger(__name__)
    client = init_bq_client()
    target_date = _calc_target_date(context)
    print(f"▶ target_date: {target_date}")
    try:
        etl_f_tracker_install(target_date=target_date, client=client)
        logger.info("✅ f_tracker_install 테스트 완료")
    except Exception as e:
        logger.error(f"❌ f_tracker_install 테스트 실패: {e}")
        raise


def run_test_first(**context):
    logger = logging.getLogger(__name__)
    client = init_bq_client()
    target_date = _calc_target_date(context)
    print(f"▶ target_date: {target_date}")
    try:
        etl_f_tracker_first(target_date=target_date, client=client)
        logger.info("✅ f_tracker_first 테스트 완료")
    except Exception as e:
        logger.error(f"❌ f_tracker_first 테스트 실패: {e}")
        raise


def run_test_common_register(**context):
    logger = logging.getLogger(__name__)
    client = init_bq_client()
    target_date = _calc_target_date(context)
    print(f"▶ target_date: {target_date}")
    try:
        etl_f_common_register(target_date=target_date, client=client)
        logger.info("✅ f_common_register 테스트 완료")
    except Exception as e:
        logger.error(f"❌ f_common_register 테스트 실패: {e}")
        raise


def run_test_adjust_register(**context):
    logger = logging.getLogger(__name__)
    client = init_bq_client()
    target_date = _calc_target_date(context)
    print(f"▶ target_date: {target_date}")
    try:
        adjust_f_common_register(target_date=target_date, client=client)
        logger.info("✅ adjust_f_common_register 테스트 완료")
    except Exception as e:
        logger.error(f"❌ adjust_f_common_register 테스트 실패: {e}")
        raise


def run_test_user_map(**context):
    logger = logging.getLogger(__name__)
    client = init_bq_client()
    target_date = _calc_target_date(context)
    print(f"▶ target_date: {target_date}")
    try:
        etl_f_user_map(target_date=target_date, client=client)
        logger.info("✅ f_user_map 테스트 완료")
    except Exception as e:
        logger.error(f"❌ f_user_map 테스트 실패: {e}")
        raise


def run_test_user_map_char(**context):
    logger = logging.getLogger(__name__)
    client = init_bq_client()
    target_date = _calc_target_date(context)
    print(f"▶ target_date: {target_date}")
    try:
        etl_f_user_map_char(target_date=target_date, client=client)
        logger.info("✅ f_user_map_char 테스트 완료")
    except Exception as e:
        logger.error(f"❌ f_user_map_char 테스트 실패: {e}")
        raise


# ── DAG 정의 ──────────────────────────────────────────────────────────────────
with DAG(
    dag_id="test_etl_f_tracker_install",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # 수동 트리거 전용
    catchup=False,
    tags=["test"],
) as dag:

    task_install = PythonOperator(
        task_id="run_etl_f_tracker_install",
        python_callable=run_test_install,
    )

    task_first = PythonOperator(
        task_id="run_etl_f_tracker_first",
        python_callable=run_test_first,
    )

    task_common_register = PythonOperator(
        task_id="run_etl_f_common_register",
        python_callable=run_test_common_register,
    )

    task_adjust_register = PythonOperator(
        task_id="run_adjust_f_common_register",
        python_callable=run_test_adjust_register,
    )

    task_user_map = PythonOperator(
        task_id="run_etl_f_user_map",
        python_callable=run_test_user_map,
    )

    task_user_map_char = PythonOperator(
        task_id="run_etl_f_user_map_char",
        python_callable=run_test_user_map_char,
    )

    # ── Task 순서 ──────────────────────────────────────────────────────────────
    [task_install, task_first] >> task_common_register >> task_adjust_register >> task_user_map >> task_user_map_char
