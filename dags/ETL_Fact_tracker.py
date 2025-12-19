from google.cloud import bigquery
from datetime import datetime, timezone, timedelta
import time
import pytz


# 빅쿼리 클라이언트 연결
client = bigquery.Client()


def etl_pre_joytracking_tracker():

    for td in target_date:
        target_date = td

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        kst = pytz.timezone('Asia/Seoul')
        start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
        end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)
        print(f"📝 시작시간 : ", start_utc, f" 📝 종료시간 : ", end_utc)

        query = f"""
        MERGE `datahub-478802.datahub.pre_joytracking_tracker` AS target
        USING
        (
          WITH AuthAccountInfo
          AS (
              SELECT joyple_game_code
                   , auth_account_name
                   , INFO.tracker_id AS tracker_account_id
                   , INFO.world_id   AS world_id
                   , INFO.user_type  AS user_type
                   , INFO.log_date   AS auth_account_log_timestamp
              FROM (
                    SELECT a.game_code         AS joyple_game_code 
                         , a.auth_account_name AS auth_account_name
                         , ARRAY_AGG(STRUCT(tracker_id, world_id, log_date, user_type)
                                     ORDER BY log_date ASC 
                                     LIMIT 1
                                    )[OFFSET(0)] AS INFO         
                    FROM `dataplatform-204306.JoyTracking.lt_pop_visit_history` AS a 
                    WHERE a.log_date >= {start_utc} 
                      AND a.log_date < {end_utc}
                      AND a.tracker_id IS NOT NULL 
                      AND a.tracker_id != ''
                      AND left(a.tracker_id,16) != '0000000000000000'
                    GROUP BY joyple_game_code, auth_account_name
                   ) AS a
          )
    
          SELECT a.joyple_game_code                      AS joyple_game_code
               , a.INFO.world_id                         AS world_id
               , a.auth_account_name                     AS auth_account_name
               , a.INFO.tracker_account_id               AS tracker_account_id
               , c.campaign                              AS campaign_name
               , a.INFO.ad_name                          AS ad_name
               , a.user_type                             AS user_type
               , timestamp(a.INFO.log_datetime_kst)      AS register_timestamp
          FROM (
                SELECT joyple_game_code
                     , auth_account_name
                     , tracker_account_id
                     , user_type
                     , ARRAY_AGG(STRUCT(world_id, tracker_account_id, ad_name, log_datetime_kst)
                                 ORDER BY log_datetime_kst DESC 
                                 LIMIT 1
                                )[OFFSET(0)] AS INFO       
                FROM(
                     SELECT a.auth_account_name                    AS auth_account_name
                          , a.joyple_game_code                     AS joyple_game_code
                          , a.world_id                             AS world_id
                          , a.tracker_account_id                   AS tracker_account_id
                          , a.user_type                            AS user_type
                          , b.ads                              AS ad_name
                          , DATETIME(b.log_date, "Asia/Seoul") AS log_datetime_kst  
                    FROM AuthAccountInfo  AS a
                    INNER JOIN `dataplatform-204306.JoyTracking.lt_click_visit_history` AS b ON (a.tracker_account_id = b.tracker_id)
                    WHERE b.log_date BETWEEN DATE_SUB(a.auth_account_log_timestamp, INTERVAL 7 DAY) AND a.auth_account_log_timestamp
                    AND b.tracker_id not like  "0000000000000000%" -- 0000000000000000% 는 조회하지 않음
                   )
                GROUP BY auth_account_name, joyple_game_code, world_id, tracker_account_id, user_type
          ) AS a
          LEFT OUTER JOIN `dataplatform-joytracking.joytracking.tb_ads_campaign` AS c 
          ON (a.INFO.ad_name = c.ads) 
          WHERE a.joyple_game_code IS NOT NULL
          AND a.INFO.ad_name between 35 and 10000 --- 테스트 데이터는 제외
        ) AS source ON target.joyple_game_code = source.joyple_game_code AND target.auth_account_name = source.auth_account_name
        WHEN NOT MATCHED BY target THEN
          INSERT (joyple_game_code, auth_account_name, tracker_account_id, campaign_name, ad_name,user_type, register_timestamp)
          VALUES (
                  source.joyple_game_code
                , source.auth_account_name
                , source.tracker_account_id
                , source.campaign_name
                , source.ad_name
                , source.user_type
                , source.register_timestamp
	      )     
    
        """

def etl_f_tracker_install():

    for td in target_date:
        target_date = td

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        kst = pytz.timezone('Asia/Seoul')
        start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
        end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)
        print(f"📝 시작시간 : ", start_utc, f" 📝 종료시간 : ", end_utc)

        query = f"""
        INSERT INTO `datahub-478802.datahub.f_tracker_install` 
            (app_id,
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

        WITH TSS AS (
        SELECT TRIM(AppID) AS app_id
                , TrackerAccountID AS tracker_account_id
                , TrackerTypeID AS tracker_type_id
                , TRIM(BundleID) AS bundle_id
                , TRIM(Platform) AS platform
                , CountryCode AS country_code
                , TRIM(MediaSource) AS media_source
                , IFNULL(IF(MediaSource = "Organic", "Organic", IF(MediaSource = "Facebook Ads", "FB", IF(MediaSource = "googleadwords_int", "Google", "Other"))), "Other") AS media_source_cat
                , IF(MediaSource = "Organic", "Organic", IF(MediaSource = "GameRoom", "Unknown", "Non-Organic"))  AS is_organic
                , TRIM(Agency) AS agency
                , CASE WHEN b.campaign_name IS NOT NULL THEN b.campaign_name ELSE (IF(a.campaign = '' OR a.campaign is null, "NULL", TRIM(a.campaign))) END AS campaign
                , CASE WHEN MediaSource = 'googleadwords_int' AND LENGTH(a.campaign) <= 11 AND a.campaign NOT LIKE '%UAC%' AND a.campaign NOT LIKE 'PRE_MAIN%' THEN b.campaign_name
                        WHEN a.campaign = '' OR a.campaign is null THEN "NULL"
                        ELSE TRIM(a.campaign)
                END AS init_campaign
                , TRIM(NORMALIZE(AdsetName, NFC)) AS adset_name
                , TRIM(NORMALIZE(AdName, NFC)) AS ad_name
                , IsRetargeting AS is_retargeting
                , TRIM(AdvertisingID) AS advertising_id
                , TRIM(IDFA) AS idfa
                , TRIM(SiteID) AS site_id
                , TRIM(Channel) AS channel
                , TRIM(CB1MediaSource) AS CB1_media_source
                , TRIM(CB1Campaign) AS CB1_campaign
                , TRIM(CB2MediaSource) AS CB2_media_source
                , TRIM(CB2Campaign) AS CB2_campaign
                , TRIM(CB3MediaSource) AS CB3_media_source
                , TRIM(CB3Campaign) AS CB3_campaign         
                , InstallTime AS install_time
                , EventTime AS event_time
                , EventType AS event_type
            FROM (
            SELECT app_id                                                                                                                    AS AppID
                , appsflyer_id                                                                                                              AS TrackerAccountID
                , 1                                                                                                                         AS TrackerTypeID     
                , bundle_id                                                                                                                 AS BundleID
                , platform                                                                                                                  AS Platform
                , UPPER(country_code)                                                                                                       AS CountryCode
                -- media_source가 null일 경우 'NULL'로 처리     
                , IFNULL(IF(appsflyer_id = "1000-0000", "GameRoom", IF(media_source = 'organic', "Organic", media_source)), "NULL")         AS MediaSource
                , af_prt                                                                                                                    AS Agency
                , IFNULL(campaign, "NULL")                                                                                                  AS Campaign 
                , af_adset                                                                                                                  AS AdsetName
                , af_ad                                                                                                                     AS AdName
                , is_retargeting                                                                                                            AS IsRetargeting
                , advertising_id                                                                                                            AS AdvertisingID
                , idfa                                                                                                                      AS IDFA
                , af_siteid                                                                                                                 AS SiteID
                , af_channel                                                                                                                AS Channel
                , contributor_1_media_source                                                                                                AS CB1MediaSource
                , contributor_1_campaign                                                                                                    AS CB1Campaign
                , contributor_2_media_source                                                                                                AS CB2MediaSource
                , contributor_2_campaign                                                                                                    AS CB2Campaign
                , contributor_3_media_source                                                                                                AS CB3MediaSource
                , contributor_3_campaign                                                                                                    AS CB3Campaign        
                , install_time                                                                                                              AS InstallTime
                , event_time                                                                                                                AS EventTime
                , event_name                                                                                                                AS EventType
            FROM `dataplatform-reporting.AppsflyerLog.V_LogsV2`
            WHERE event_time >= '2025-12-01' and event_time < '2025-12-02'
                AND event_name in ('install', 'reinstall', 're-attribution', 're-engagement')
                -- AND install_time >= "2019-12-19 00:48:35.827000 UTC"  
                AND event_time   >= "2019-12-19 00:48:35.827000 UTC"  
            UNION ALL
            SELECT app_id                                                                                                                    AS AppID
                , appsflyer_id                                                                                                              AS TrackerAccountID
                , 1                                                                                                                         AS TrackerTypeID
                , bundle_id                                                                                                                 AS BundleID
                , platform                                                                                                                  AS Platform
                , UPPER(country_code)                                                                                                       AS CountryCode
                , IFNULL(media_source, 'Organic')                                                                                           AS MediaSource 
                , af_prt                                                                                                                    AS Agency
                , IFNULL(campaign, "NULL")                                                                                                  AS Campaign
                , af_adset                                                                                                                  AS AdsetName
                , af_ad                                                                                                                     AS AdName  
                , is_retargeting                                                                                                            AS IsRetargeting
                , advertising_id                                                                                                            AS AdvertisingID
                , idfa                                                                                                                      AS IDFA
                , af_siteid                                                                                                                 AS SiteID
                , af_channel                                                                                                                AS Channel
                , contributor_1_media_source                                                                                                AS CB1MediaSource
                , contributor_1_campaign                                                                                                    AS CB1Campaign
                , contributor_2_media_source                                                                                                AS CB2MediaSource
                , contributor_2_campaign                                                                                                    AS CB2Campaign
                , contributor_3_media_source                                                                                                AS CB3MediaSource
                , contributor_3_campaign                                                                                                    AS CB3Campaign       
                , install_time                                                                                                              AS InstallTime 
                , event_time                                                                                                                AS EventTime
                , event_name                                                                                                                AS EventType
            FROM `dataplatform-204306.AppsflyerLog.installs_report`
            WHERE event_time >= '2025-12-01' and event_time < '2025-12-02'
            AND event_name in ('install', 'reinstall', 're-attribution', 're-engagement')
            ) AS a
            LEFT JOIN `datahub-478802.datahub.dim_google_campaign` AS b ON a.Campaign = b.campaign_id
        )

        SELECT TRIM(INFO.app_id) AS app_id
            , b.joyple_game_code
            , b.market_id
            , tracker_account_id
            , tracker_type_id
            , INFO.bundle_id
            , INFO.platform
            , INFO.country_code
            , INFO.media_source
            , INFO.media_source_cat
            , INFO.is_organic
            , INFO.agency
            , INFO.campaign
            , INFO.init_campaign
            , INFO.adset_name
            , INFO.ad_name
            , INFO.is_retargeting
            , INFO.advertising_id
            , INFO.idfa
            , INFO.site_id
            , INFO.channel
            , INFO.CB1_media_source
            , INFO.CB1_campaign
            , INFO.CB2_media_source
            , INFO.CB2_campaign
            , INFO.CB3_media_source
            , INFO.CB3_campaign
            , INFO.install_time
            , INFO.event_time
            , INFO.event_type
            , EXTRACT(DATE FROM DATETIME(INFO.install_time, "+09:00")) AS install_datekey
            FROM (
                SELECT tracker_account_id
                    , tracker_type_id
                    , ARRAY_AGG(
                    STRUCT(
                        app_id,
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
                        event_type
                    )
                    ORDER BY install_time ASC
                    LIMIT 1
                    )[OFFSET(0)] AS INFO  
                FROM TSS
                WHERE app_id IS NOT NULL
                AND tracker_account_id IS NOT NULL
                AND tracker_account_id <> ""
                AND event_type = "install"
                GROUP BY tracker_account_id, tracker_type_id
            ) AS a
            LEFT OUTER JOIN `datahub-478802.datahub.dim_app_id` AS b
            ON a.INFO.app_id = b.app_id

        """
        client.query(query)
        print(f"■ {target_date.strftime('%Y-%m-%d')} f_tracker_install Batch 완료")
    
    print("✅ f_tracker_install ETL 완료")
    return True


def etl_f_tracker_re_engagement():

    for td in target_date:
        target_date = td

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        kst = pytz.timezone('Asia/Seoul')
        start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
        end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)
        print(f"📝 시작시간 : ", start_utc, f" 📝 종료시간 : ", end_utc)

        query = f"""
        INSERT INTO `datahub-478802.datahub.f_tracker_re_engagement` 
            (app_id,
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

        WITH TSS AS (
        SELECT TRIM(AppID) AS app_id
                , TrackerAccountID AS tracker_account_id
                , TrackerTypeID AS tracker_type_id
                , TRIM(BundleID) AS bundle_id
                , TRIM(Platform) AS platform
                , CountryCode AS country_code
                , TRIM(MediaSource) AS media_source
                , IFNULL(IF(MediaSource = "Organic", "Organic", IF(MediaSource = "Facebook Ads", "FB", IF(MediaSource = "googleadwords_int", "Google", "Other"))), "Other") AS media_source_cat
                , IF(MediaSource = "Organic", "Organic", IF(MediaSource = "GameRoom", "Unknown", "Non-Organic"))  AS is_organic
                , TRIM(Agency) AS agency
                , CASE WHEN b.campaign_name IS NOT NULL THEN b.campaign_name ELSE (IF(a.campaign = '' OR a.campaign is null, "NULL", TRIM(a.campaign))) END AS campaign
                , CASE WHEN MediaSource = 'googleadwords_int' AND LENGTH(a.campaign) <= 11 AND a.campaign NOT LIKE '%UAC%' AND a.campaign NOT LIKE 'PRE_MAIN%' THEN b.campaign_name
                        WHEN a.campaign = '' OR a.campaign is null THEN "NULL"
                        ELSE TRIM(a.campaign)
                END AS init_campaign
                , TRIM(NORMALIZE(AdsetName, NFC)) AS adset_name
                , TRIM(NORMALIZE(AdName, NFC)) AS ad_name
                , IsRetargeting AS is_retargeting
                , TRIM(AdvertisingID) AS advertising_id
                , TRIM(IDFA) AS idfa
                , TRIM(SiteID) AS site_id
                , TRIM(Channel) AS channel
                , TRIM(CB1MediaSource) AS CB1_media_source
                , TRIM(CB1Campaign) AS CB1_campaign
                , TRIM(CB2MediaSource) AS CB2_media_source
                , TRIM(CB2Campaign) AS CB2_campaign
                , TRIM(CB3MediaSource) AS CB3_media_source
                , TRIM(CB3Campaign) AS CB3_campaign         
                , InstallTime AS install_time
                , EventTime AS event_time
                , EventType AS event_type
            FROM (
            SELECT app_id                                                                                                                    AS AppID
                , appsflyer_id                                                                                                              AS TrackerAccountID
                , 1                                                                                                                         AS TrackerTypeID     
                , bundle_id                                                                                                                 AS BundleID
                , platform                                                                                                                  AS Platform
                , UPPER(country_code)                                                                                                       AS CountryCode
                -- media_source가 null일 경우 'NULL'로 처리     
                , IFNULL(IF(appsflyer_id = "1000-0000", "GameRoom", IF(media_source = 'organic', "Organic", media_source)), "NULL")         AS MediaSource
                , af_prt                                                                                                                    AS Agency
                , IFNULL(campaign, "NULL")                                                                                                  AS Campaign 
                , af_adset                                                                                                                  AS AdsetName
                , af_ad                                                                                                                     AS AdName
                , is_retargeting                                                                                                            AS IsRetargeting
                , advertising_id                                                                                                            AS AdvertisingID
                , idfa                                                                                                                      AS IDFA
                , af_siteid                                                                                                                 AS SiteID
                , af_channel                                                                                                                AS Channel
                , contributor_1_media_source                                                                                                AS CB1MediaSource
                , contributor_1_campaign                                                                                                    AS CB1Campaign
                , contributor_2_media_source                                                                                                AS CB2MediaSource
                , contributor_2_campaign                                                                                                    AS CB2Campaign
                , contributor_3_media_source                                                                                                AS CB3MediaSource
                , contributor_3_campaign                                                                                                    AS CB3Campaign        
                , install_time                                                                                                              AS InstallTime
                , event_time                                                                                                                AS EventTime
                , event_name                                                                                                                AS EventType
            FROM `dataplatform-reporting.AppsflyerLog.V_LogsV2`
            WHERE event_time >= '2025-12-01' and event_time < '2025-12-02'
                AND event_name in ('install', 'reinstall', 're-attribution', 're-engagement')
                -- AND install_time >= "2019-12-19 00:48:35.827000 UTC"  
                AND event_time   >= "2019-12-19 00:48:35.827000 UTC"  
            UNION ALL
            SELECT app_id                                                                                                                    AS AppID
                , appsflyer_id                                                                                                              AS TrackerAccountID
                , 1                                                                                                                         AS TrackerTypeID
                , bundle_id                                                                                                                 AS BundleID
                , platform                                                                                                                  AS Platform
                , UPPER(country_code)                                                                                                       AS CountryCode
                , IFNULL(media_source, 'Organic')                                                                                           AS MediaSource 
                , af_prt                                                                                                                    AS Agency
                , IFNULL(campaign, "NULL")                                                                                                  AS Campaign
                , af_adset                                                                                                                  AS AdsetName
                , af_ad                                                                                                                     AS AdName  
                , is_retargeting                                                                                                            AS IsRetargeting
                , advertising_id                                                                                                            AS AdvertisingID
                , idfa                                                                                                                      AS IDFA
                , af_siteid                                                                                                                 AS SiteID
                , af_channel                                                                                                                AS Channel
                , contributor_1_media_source                                                                                                AS CB1MediaSource
                , contributor_1_campaign                                                                                                    AS CB1Campaign
                , contributor_2_media_source                                                                                                AS CB2MediaSource
                , contributor_2_campaign                                                                                                    AS CB2Campaign
                , contributor_3_media_source                                                                                                AS CB3MediaSource
                , contributor_3_campaign                                                                                                    AS CB3Campaign       
                , install_time                                                                                                              AS InstallTime 
                , event_time                                                                                                                AS EventTime
                , event_name                                                                                                                AS EventType
            FROM `dataplatform-204306.AppsflyerLog.installs_report`
            WHERE event_time >= '2025-12-01' and event_time < '2025-12-02'
            AND event_name in ('install', 'reinstall', 're-attribution', 're-engagement')
            ) AS a
            LEFT JOIN `datahub-478802.datahub.dim_google_campaign` AS b ON a.Campaign = b.campaign_id
        )

        SELECT TRIM(INFO.app_id) AS app_id
            , b.joyple_game_code
            , b.market_id
            , tracker_account_id
            , tracker_type_id
            , INFO.bundle_id
            , INFO.platform
            , INFO.country_code
            , INFO.media_source
            , INFO.media_source_cat
            , INFO.is_organic
            , INFO.agency
            , INFO.campaign
            , INFO.init_campaign
            , INFO.adset_name
            , INFO.ad_name
            , INFO.is_retargeting
            , INFO.advertising_id
            , INFO.idfa
            , INFO.site_id
            , INFO.channel
            , INFO.CB1_media_source
            , INFO.CB1_campaign
            , INFO.CB2_media_source
            , INFO.CB2_campaign
            , INFO.CB3_media_source
            , INFO.CB3_campaign
            , INFO.install_time
            , INFO.event_time
            , INFO.event_type
            , EXTRACT(DATE FROM DATETIME(INFO.install_time, "+09:00")) AS install_datekey
            FROM (
                SELECT tracker_account_id
                    , tracker_type_id
                    , ARRAY_AGG(
                    STRUCT(
                        app_id,
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
                        event_type
                    )
                    ORDER BY install_time ASC
                    LIMIT 1
                    )[OFFSET(0)] AS INFO  
                FROM TSS
                WHERE app_id IS NOT NULL
                AND tracker_account_id IS NOT NULL
                AND tracker_account_id <> ""
                AND event_type in ('reinstall', 're-attribution', 're-engagement')
                GROUP BY tracker_account_id, tracker_type_id
            ) AS a
            LEFT OUTER JOIN `datahub-478802.datahub.dim_app_id` AS b
            ON a.INFO.app_id = b.app_id

        """
        client.query(query)
        print(f"■ {target_date.strftime('%Y-%m-%d')} f_tracker_re_engagement Batch 완료")
    
    print("✅ f_tracker_re_engagement ETL 완료")
    return True


def etl_f_cost_campaign_rule():

    truncate_query = f"""
    TRUNCATE TABLE `datahub-478802.datahub.f_cost_campaign_rule`
    """

    query = f"""

-- TRUNCATE TABLE `datahub-478802.datahub.f_common_register`


insert into `datahub-478802.datahub.f_common_register`
(
  reg_datekey,
  reg_datetime,
  game_id,
  world_id,
  joyple_game_code,
  auth_method_id,
  auth_account_name,
  tracker_account_id,
  tracker_type_id,
  device_id,
  reg_country_code,
  market_id,
  os_id,
  platform_device_type,
  app_id,
  bundle_id,
  install_country_code,
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
SELECT
a.AuthAccountRegDateKST as reg_datekey,
DATETIME(a.AuthAccountRegTimestamp, "Asia/Seoul") as reg_datetime,
a.GameID as game_id,
a.WorldID as world_id,
a.JoypleGameID as joyple_game_code,
a.AuthMethodID as auth_method_id,
a.AuthAccountName as auth_account_name,
a.TrackerAccountID as tracker_account_id,
a.TrackerTypeID as tracker_type_id,
c.INFO.DeviceID as device_id,
a.CountryCode as reg_country_code,
a.MarketID as market_id,
b.OSID as os_id,
c.INFO.PlatformDeviceType as platform_device_type,
a.AppID as app_id,
a.BundleID as bundle_id,
c.INFO.CountryCode as install_country_code,
a.MediaSource as media_source,
d.MediaSourceCat as media_source_cat,
if(J.auth_account_name is not null, 'Non-Organic',a.IsOrganic) as is_organic,
a.Agency as agency,
coalesce(J.Campaign_name, a.Campaign) as campaign,
coalesce(J.Campaign_name, a.InitCampaign) as init_camapign,
a.AdsetName as adset_name,
coalesce(cast(J.ad_name as string), a.AdName) as ad_name,
a.IsRetargeting as is_retargeting,
a.AdvertisingID as advertising_id,
a.IDFA as idfa,
a.SiteID as site_id,
a.Channel as channel,
a.CB1MediaSource as CB1_media_source,
a.CB1Campaign as CB1_campaign,
a.CB2MediaSource as CB2_media_source,
a.CB2Campaign as CB2_campaign,
a.CB3MediaSource as CB3_media_source,
a.CB3Campaign as CB3_campaign,
a.TrackerAccountInstallTimestamp as install_time,
a.TrackerAccountInstallTimestamp as event_time,
'install' as event_type,
a.TrackerAccountInstallDateKST as install_datekey
FROM dataplatform-reporting.DataService.T_0316_0000_AuthAccountInfo_V as a
left join
dataplatform-reporting.DataService.T_0210_0000_OS_V as b
on a.OS = b.OSNameLower
left join
(
  select TrackerAccountID, TrackerTypeID,
  ARRAY_AGG(STRUCT(DeviceID, CountryCode, PlatformDeviceType) ORDER BY UpdatedTimestamp ASC LIMIT 1)[OFFSET(0)] AS INFO
  from dataplatform-reporting.DataService.T_0272_0000_TrackerAccountInfo_V
  GROUP BY TrackerAccountID, TrackerTypeID
)
as c
on a.TrackerAccountID = c.TrackerAccountID and a.TrackerTypeID = c.TrackerTypeID
left join
dataplatform-reporting.DataService.T_0273_0000_TrackerAccountFirst_V as d
on a.TrackerAccountID = d.TrackerAccountID and a.TrackerTypeID = d.TrackerTypeID
left join (
           select a.*, b.* except(campaign_name, joyple_game_code)
           from `datahub-478802.datahub.pre_joytracking_tracker` as a
           left join `datahub-478802.datahub.dim_pccampaign_list_joytracking` as b
           on a.campaign_name = b.campaign_name and a.joyple_game_code = b.joyple_game_code) as J
on a.joyplegameid = j.joyple_game_code and a.authaccountname = j.auth_account_name
    """

    client.query(truncate_query)
    time.sleep(5)
    client.query(query)

    print("✅ f_cost_campaign_rule ETL 완료")

