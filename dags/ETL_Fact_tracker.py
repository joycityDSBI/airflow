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
            WHERE event_time >= {start_utc} and event_time < {end_utc}
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
            WHERE event_time >= {start_utc} and event_time < {end_utc}
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
            , DATETIME(INFO.install_time, 'Asia/Seoul') as install_time
            , DATETIME(INFO.event_time,, 'Asia/Seoul') as event_time
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
            WHERE event_time >= {start_utc} and event_time < {end_utc}
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
            WHERE event_time >= {start_utc} and event_time < {end_utc}
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
            , DATETIME(INFO.install_time, 'Asia/Seoul') as install_time
            , DATETIME(INFO.event_time,, 'Asia/Seoul') as event_time
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
        INSERT INTO `datahub-478802.datahub.f_cost_campaign_rule`
            (
            joyple_game_code
            , upload_timestamp
            , cmpgn_dt
            , gcat
            , game_id
            , country_code
            , currency 
            , cost
            , cost_currency_uptdt
            , currency_rate
            , cost_currency
            , campaign_name
            , campaign_id
            , adset_name
            , adset_id
            , ad_name
            , ad_id
            , impressions
            , clicks
            , mas_cmpgn_yn
            , create_timestamp
            , update_timestamp
            , upload_agent
            , user_id
            , media_category
            , product_category
            , media
            , media_detail
            , optim
            , etc_category
            , os
            , location
            , creative_no
            , device
            , setting_title
            , landing_title
            , ad_unit
            , mediation
            , pre_yn
            , pre_cate
            , media_group
            , target_group  
            )
        
        WITH CostCampaignRule
        AS (
        SELECT a.* 
                , Category                                                        AS pre_cat
                , Campaign_Start_Date                                               AS cmpgn_start
                , Campaign_End_Date                                                 AS cmpgn_end
                , Cost_Start_Date                                                   AS pre_date
                , CASE WHEN cmpgn_nm LIKE '%Credit%' THEN 0
                    ELSE cost_currency 
                END AS cost_currency_uptdt 
        FROM (SELECT upload_time
                    , cmpgn_dt
                    , gcat
                    , CASE WHEN game_id in ('MTSG','DS') THEN 'DS' 
                            when game_id in ('JT', 'JTWN') Then 'JTWN'
                            ELSE game_id end as game_id -- 향후데이터 수정해주면 제거해야할 로직
                    , joyple_game_code AS JoypleGameID
                    , country
                    , currency
                    , cost
                    , currency_rate
                    , cost_currency
                    , case
                            -- 과거 크레딧 캠페인 이후 집행할 경우 신규로 다시 집행하거나 하는 액션이 없었기 때문에 크레딧 지원기간, 금액보다 해당 캠페인의 집행기간 및 총금액이 높을 수 있음
                            when game_id = 'CFWZ' and cmpgn_nm = 'US_And_FB_MAIA_AAA_210618' and cmpgn_dt between '2021-06-25' and '2021-07-08' then 'US_And_FB_MAIA_Credit(AAA)_210618'
                            when game_id = 'CFWZ' and cmpgn_nm = 'WW_IOS_FB_IOS14_AEO_AAA_210901' and cmpgn_dt between '2021-09-01' and '2021-10-08' then 'WW_IOS_FB_IOS14_AEO_Credit(AAA)_210901'
                            when game_id = 'GBTW' and cmpgn_nm = 'TopTier_IOS_FB_IOS14_AEO_AAA_211001' and cmpgn_dt between '2021-10-01' and '2021-11-30' then 'TopTier_IOS_FB_IOS14_AEO_Credit(AAA)_211001'
                            when game_id = 'GBTW' and cmpgn_nm = 'US_And_FB_CEO(Pecan)_220414' and cmpgn_dt between '2022-04-19' and '2022-05-02' then 'US_And_FB_CEO(Pecan)_Credit_220414'
                            when game_id = 'GBTW' and cmpgn_nm = 'US_IOS_FB_IOS14_VO_210520' and cmpgn_dt between '2021-05-20' and '2021-06-08' then 'US_IOS_FB_IOS14_VO_Credit_210520'
                            when game_id = 'GBTW' and cmpgn_nm = 'WW_And_FB_IAA_CEO(Install)_AAA_221031' then 'WW_And_FB_IAA_CEO(Install)_Credit(AAA)_221031' -- 해당 캠페인 크레딧으로 시작하여 시작되었기 때문에 크레딧
                            when game_id = 'POTC' and cmpgn_nm = 'US_ALL_FB-Branding_LAL_Awareness-R&F_210512' and cmpgn_dt between '2021-05-01' and '2021-06-08' then 'US_ALL_FB-Branding_LAL_Awareness-R&F_Credit_210512'
                            when game_id = 'POTC' and cmpgn_nm = 'US_And_FB_LAL_AEO_210430' and cmpgn_dt between '2021-05-01' and '2021-06-08' then 'US_And_FB_LAL_AEO_Credit_210430'
                            when game_id = 'POTC' and cmpgn_nm = 'US_And_FB_LAL_MAIA_210430' and cmpgn_dt between '2021-05-01' and '2021-06-08' then 'US_And_FB_LAL_MAIA_Credit_210430'
                            when game_id = 'POTC' and cmpgn_nm = 'US_And_FB_LAL_VO_210430' and cmpgn_dt between '2021-05-01' and '2021-08-06' then 'US_And_FB_LAL_VO_Credit_210430'
                            when game_id = 'POTC' and cmpgn_nm = 'US_IOS_Snapchat.Self_IOS14_CPM_tCPI_220317' and cmpgn_dt between '2022-03-18' and '2022-04-01' then 'US_IOS_Snapchat.Self_IOS14_CPM_tCPI_Credit_220317'
                            when game_id = 'POTC' and cmpgn_nm = 'WW_And_ACe_tCPA_Purchase_211129' and cmpgn_dt between '2021-11-29' and '2021-12-30' then 'WW_And_ACe_tCPA_Credit(Purchase)_211129'
                            when game_id = 'POTC' and cmpgn_nm = 'DE_ALL_FB-Branding_LAL_Awareness_220525' and cmpgn_dt between '2022-05-25' and '2022-06-08' then 'DE_ALL_FB-Branding_LAL_Awareness_Credit_220525'
                            when game_id = 'POTC' and cmpgn_nm = 'FR_ALL_FB-Branding_LAL_Awareness_220525' and cmpgn_dt between '2022-05-25' and '2022-06-08' then 'FR_ALL_FB-Branding_LAL_Awareness_Credit_220525'
                            when game_id = 'POTC' and cmpgn_nm = 'UK_ALL_FB-Branding_LAL_Awareness_220525' and cmpgn_dt between '2022-05-25' and '2022-06-08' then 'UK_ALL_FB-Branding_LAL_Awareness_Credit_220525'
                            when game_id = 'POTC' and cmpgn_nm = 'US_ALL_FB-Branding_LAL_Awareness_220525' and cmpgn_dt between '2022-05-25' and '2022-06-08' then 'US_ALL_FB-Branding_LAL_Awareness_Credit_220525'
                        else cmpgn_nm end as cmpgn_nm -- 향후데이터 수정해주면 제거해야할 로직
                    , cmpgn_id
                    , adset_nm
                    , adset_id
                    , ad_nm
                    , ad_id
                    , mas_cmpgn_yn
                    , creat_dt
                    , uptdt_dt
                    , upload_agent
                    , user_id
                    , media_category
                    , product_category
                    , media
                    , media_detail
                    , optim2 as optim
                    , etc_category
                    , os
                    , IF(location = 'UK', 'GB', location) AS location -- 향후 데이터 수정해주면 제거해야할 로직
                    , creative_no
                    , device
                    , setting_title
                    , landing_title
                    , ad_unit
                    , mediation
                    , impressions
                    , clicks 
                    , CASE WHEN media_category LIKE '%-Pre'   THEN true
                            WHEN media_category LIKE '%-Pre-%' THEN true
                            WHEN media_category LIKE 'Pre-%'   THEN true
                            WHEN media_category LIKE 'pre-%'   THEN true
                            WHEN media_category LIKE '%-pre'   THEN true
                            WHEN media_category IN ('Preregister','Update-Preregister','Update -Preregister','Google-ACP') THEN true
                            WHEN media IN ('GL-PC-UpdatePre','GL-PC-Pre','FB-PC-UpdatePre','FB-PC-Pre') THEN true  -- 210923 수정한 부분
                            ELSE false 
                        END AS pre_yn   -- 요청은 했으나 반영될 가능성이 적어 전처리가 필요한 로직.
                    , CASE WHEN game_id = 'KOFS' and country = 'JP' then 1 
                            WHEN game_id = 'RESU' and country IN ('KR', 'TW', 'HK', 'MO', 'VN', 'ID', 'BN', 'MM', 'MN') then 1
                            ELSE 0 
                            END AS extra_process_required -- 향후 pre_cost_campaign_rule_pre_book 테이블에 country 추가되면 삭제해야할 로직
                    , CASE WHEN media_category in ('Google', 'Google-ACP', 'Google-PC', 'Google-Re')                 THEN 'Google'
                            WHEN media_category in ('Facebook', 'Facebook-3rd', 'Facebook-Gaming', 'Facebook-PC', 'Facebook-Playable', 'Facebook-Re') THEN 'FB'
                            WHEN media_category in ('ADNW','ADNW-Re')                   THEN 'ADNW'
                            WHEN LOWER(gcat) in ('organic','unknown')   THEN 'Organic'
                            ELSE 'Other' 
                    END AS media_group  -- 각 빅미디어 모든 매체카테고리 추가
                    , case when etc_category = 'L&F' then '그룹없음'
                            when (media_category = 'Facebook' and gcat = 'UA' and product_category is null and optim = 'NONE' and optim2 = 'VO') then 'UA-HVU'
                            when (media_category = 'Facebook' and gcat = 'UA' and product_category is null and optim = 'NONE' and optim2 = 'MAIA') then 'UA-Install'
                            when (media_category = 'Facebook' and gcat = 'UA' and product_category is null and optim = 'NONE' and optim2 = 'AEO') then 'UA-VU'
                            when (media_category = 'Mytarget.Self' and gcat = 'UA' and product_category is null and optim = 'MAIA') then 'UA-Install'
                            when (optim in ('CEO(Pecan)', 'CEO(Model)'))  then 'UA-VU'
                    else target_group 
                    end as target_group -- 데이터 처리 전까지만 하드코딩 대응 수정된 이후에 하드코딩은 삭제 예정
            FROM (select * ,
                        case when optim  = 'NONE' and adset_nm like '%MAIA%' then 'MAIA'
                            when  optim  = 'NONE' and   adset_nm like '%AEO%' then 'AEO'
                            when  optim  = 'NONE' and  adset_nm like '%VO%' then 'VO'
                        else optim end as optim2 -- 해당케이스 처리하지 못해 살려야함.(mas 캠페인 처리 로직상 ad)
                    from (select A.*, B.joyple_game_code
                        from `dataplatform-bdts.mas.v_cost_campaign_rule_group` as a
                        LEFT  JOIN `dataplatform-bdts.mas.game_id` AS B
                        ON A.game_id = B.game_id
                        )   
                )
            ) AS a
            LEFT OUTER JOIN `datahub-478802.datahub.pre_cost_campaign_rule_pre_book` AS c 
            ON (pre_yn = true AND a.JoypleGameID = c.joyple_game_code AND a.cmpgn_dt between campaign_start_date AND campaign_end_date and a.extra_process_required = c.extra_process_required)
        )
        ,
        T_Final 
        AS 
        (
        -- 사전예약이 아닌 원래 데이터들
        SELECT JoypleGameID            AS joyple_game_code
            , upload_time             AS upload_timestamp
            , cmpgn_dt                AS cmpgn_dt
            , gcat                    AS gcat
            , game_id                 AS game_id
            , country                 AS country_code
            , currency                AS currency
            , cost                    AS cost
            , cost_currency_uptdt     AS cost_currency_uptdt
            , currency_rate           AS currency_rate
            , cost_currency           AS cost_currency
            , cmpgn_nm                AS campaign_name
            , cmpgn_id                AS campaign_id
            , adset_nm                AS adset_name
            , adset_id                AS adset_id
            , ad_nm                   AS ad_name
            , ad_id                   AS ad_id
            , impressions             AS impressions
            , clicks                  AS clicks
            , mas_cmpgn_yn            AS mas_cmpgn_yn
            , creat_dt                AS create_timestamp
            , uptdt_dt                AS update_timestamp
            , upload_agent            AS upload_agent
            , user_id                 AS user_id
            , media_category          AS media_category
            , product_category        AS product_category
            , media                   AS media
            , media_detail            AS media_detail
            , optim                   AS optim
            , etc_category            AS etc_category
            , os                      AS os
            , location                AS location
            , creative_no             AS creative_no
            , device                  AS device
            , setting_title           AS setting_title
            , landing_title           AS landing_title
            , ad_unit                 AS ad_unit
            , mediation               AS mediation
            , pre_yn                  AS pre_yn
            , 'NULL'                  AS pre_cate
            , media_group             AS media_group
            , target_group            AS target_group
        FROM CostCampaignRule 
        WHERE pre_yn = false
        
        UNION ALL -- 사전예약 전처리 데이터
        
        SELECT a.JoypleGameID          AS joyple_game_code
            , upload_time             AS upload_timestamp
            , CostStartDateInterval   AS cmpgn_dt
            , gcat                    AS gcat
            , a.game_id               AS game_id
            , country                 AS country_code
            , currency                AS currency
            , cost_d                  AS cost
            , costcurrencyuptdt_d     AS cost_currency_uptdt     
            , currency_rate           AS currency_rate
            , cost_currency_d         AS cost_currency
            , cmpgn_nm                AS campaign_name
            , cmpgn_id                AS campaign_id
            , adset_nm                AS adset_name
            , adset_id                AS adset_id
            , ad_nm                   AS ad_name
            , ad_id                   AS ad_id
            , impressions             AS impressions
            , clicks                  AS clicks
            , mas_cmpgn_yn            AS mas_cmpgn_yn
            , creat_dt                AS create_timestamp
            , uptdt_dt                AS update_timestamp
            , upload_agent            AS upload_agent
            , user_id                 AS user_id
            , media_category          AS media_category
            , product_category        AS product_category
            , media                   AS media
            , media_detail            AS media_detail
            , optim                   AS optim
            , etc_category            AS etc_category
            , os                      AS os
            , location                AS location
            , creative_no             AS creative_no
            , device                  AS device
            , setting_title           AS setting_title
            , landing_title           AS landing_title
            , ad_unit                 AS ad_unit
            , mediation               AS mediation
            , pre_yn                  AS pre_yn
            , a.pre_cat               AS pre_cate
            , media_group             AS media_group
            , target_group            AS target_group          
        FROM (
            SELECT JoypleGameID
                , upload_time
                , gcat
                , game_id
                , country
                , currency
                , currency_rate
                , cmpgn_nm
                , cmpgn_id
                , adset_nm
                , adset_id
                , ad_nm
                , ad_id
                , impressions    
                , clicks         
                , mas_cmpgn_yn
                , creat_dt
                , uptdt_dt
                , upload_agent
                , user_id
                , media_category
                , product_category
                , media
                , media_detail
                , optim
                , etc_category
                , os
                , location
                , creative_no
                , device
                , setting_title
                , landing_title
                , ad_unit
                , mediation
                , pre_yn
                , pre_cat
                , pre_date
                , sum(cost_currency) / 7 AS cost_currency_d
                , sum(cost) / 7          AS cost_d
                , sum(cost_currency_uptdt) / 7    AS costcurrencyuptdt_d       
                , media_group             
                , target_group                              
            FROM CostCampaignRule 
            WHERE pre_cat is not null
            group by JoypleGameID
                    , upload_time
                    , gcat
                    , game_id
                    , country
                    , currency
                    , currency_rate
                    , cmpgn_nm
                    , cmpgn_id
                    , adset_nm
                    , adset_id
                    , ad_nm
                    , ad_id
                    , impressions 
                    , clicks      
                    , mas_cmpgn_yn
                    , creat_dt
                    , uptdt_dt
                    , upload_agent
                    , user_id
                    , media_category
                    , product_category
                    , media
                    , media_detail
                    , optim
                    , etc_category
                    , os
                    , location
                    , creative_no
                    , device
                    , setting_title
                    , landing_title
                    , ad_unit
                    , mediation
                    , pre_yn
                    , pre_cat
                    , pre_date
                    , media_group
                    , target_group
        ) AS a
        LEFT OUTER JOIN  
        (
            SELECT Joyple_Game_code, Category, Cost_Start_Date, CostStartDateInterval
            FROM `datahub-478802.datahub.pre_cost_campaign_rule_pre_book`
                , unnest(generate_date_array(date(Cost_Start_Date), date_add(date(Cost_Start_Date), interval 6 day),interval 1 day)) AS CostStartDateInterval
        ) AS b ON (a.JoypleGameID = b.Joyple_Game_code and a.pre_cat = b.Category and a.pre_date = b.Cost_Start_Date) 
        
        UNION ALL -- 사전예약 캠페인이긴 하나 처리가 안 된 것들
        SELECT JoypleGameID          AS joyple_game_code
            , upload_time           AS upload_timestamp
            , cmpgn_dt              AS cmpgn_dt
            , gcat                  AS gcat
            , game_id               AS game_id
            , country               AS country_code
            , currency              AS currency
            , cost                  AS cost
            , cost_currency_uptdt   AS cost_currency_uptdt
            , currency_rate         AS currency_rate
            , cost_currency         AS cost_currency
            , cmpgn_nm              AS campaign_name
            , cmpgn_id              AS campaign_id
            , adset_nm              AS adset_name
            , adset_id              AS adset_id
            , ad_nm                 AS ad_name
            , ad_id                 AS ad_id
            , impressions           AS impressions
            , clicks                AS clicks
            , mas_cmpgn_yn          AS mas_cmpgn_yn
            , creat_dt              AS create_timestamp
            , uptdt_dt              AS update_timestamp
            , upload_agent          AS upload_agent
            , user_id               AS user_id
            , media_category        AS media_category
            , product_category      AS product_category
            , media                 AS media
            , media_detail          AS media_detail
            , optim                 AS optim
            , etc_category          AS etc_category
            , os                    AS os
            , location              AS location
            , creative_no           AS creative_no
            , device                AS device
            , setting_title         AS setting_title
            , landing_title         AS landing_title
            , ad_unit               AS ad_unit
            , mediation             AS mediation
            , pre_yn                AS pre_yn
            , 'NULL'                AS pre_cate
            , media_group           AS media_group
            , target_group          AS target_group      
        FROM CostCampaignRule 
        WHERE pre_yn = true 
            AND pre_cat is null
        )
        
        SELECT a.joyple_game_code
            , a.upload_timestamp
            , a.cmpgn_dt
            , COALESCE(PC.Gcat, a.Gcat)           AS gcat
            , game_id
            , IF(country_code = 'UK', 'GB', country_code) AS country_code -- mas 데이터 수정되면 삭제 필요 
            , Currency
            , Cost
            , cost_currency_uptdt
            , currency_rate
            , cost_currency
            , a.campaign_name
            , campaign_id
            , adset_name
            , adset_id
            , ad_name
            , ad_id
            , impressions
            , clicks
            , COALESCE(PC.mas_cmpgn_yn, a.mas_cmpgn_yn)           AS mas_cmpgn_yn
            , create_timestamp
            , a.update_timestamp
            , upload_agent
            , user_id
            , COALESCE(PC.media_category, a.media_category)     AS media_category
            , COALESCE(PC.product_category, a.product_category) AS product_category
            , COALESCE(PC.Media, a.Media)                       AS media
            , COALESCE(PC.media_detail, a.media_detail)         AS media_detail
            , COALESCE(PC.Optim, a.Optim)                       AS optim
            , COALESCE(PC.etc_category, a.etc_category)         AS etc_category
            , COALESCE(PC.os_cam, a.OS)                         AS os
            , location
            , creative_no
            , device
            , setting_title
            , landing_title
            , ad_unit
            , mediation
            , pre_yn
            , pre_cate
            , COALESCE(PC.media_group,  a.media_group)           AS media_group
            , COALESCE(PC.target_group, a.target_group)         AS target_group   
        FROM T_Final as A
        LEFT JOIN (select *
                from `datahub-478802.datahub.dim_pccampaign_list_joytracking`) as PC -- PC캠페인 캠페인 정보 수정
        on A.joyple_game_code = PC.joyple_game_code and A.campaign_name = PC.campaign_name
        """
    client.query(truncate_query)
    time.sleep(5)
    client.query(query)
    print("✅ f_cost_campaign_rule ETL 완료")