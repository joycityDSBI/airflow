from google.cloud import bigquery
from datetime import datetime, timezone, timedelta
import time
import pytz



def etl_pre_joytracking_tracker(target_date:list, client):

    kst = pytz.timezone('Asia/Seoul')

    for td_str in target_date:
        # [수정 1] 문자열(String)을 datetime 객체로 변환
        # 넘어오는 날짜 형식이 'YYYY-MM-DD'라고 가정합니다.
        try:
            current_date_obj = datetime.strptime(td_str, "%Y-%m-%d")
        except ValueError:
            # 형식이 다를 경우에 대한 예외처리 (예: 시간까지 포함된 경우 등)
            # 필요에 따라 포맷을 수정하세요 ("%Y-%m-%d %H:%M:%S")
            print(f"⚠️ 날짜 형식이 잘못되었습니다: {td_str}")
            continue

        # [수정 2] pytz 라이브러리 사용 시 .replace(tzinfo=...) 보다는 .localize() 권장
        # .replace는 썸머타임이나 역사적 시간대 변경을 제대로 처리 못할 수 있음
        
        # KST 00:00:00 설정 (localize 사용)
        start_kst = kst.localize(current_date_obj)
        
        # KST -> UTC 변환
        start_utc = start_kst.astimezone(pytz.UTC)
        
        # 종료 시간 계산 (하루 뒤)
        end_kst = start_kst + timedelta(days=1)
        end_utc = end_kst.astimezone(pytz.UTC)

        print(f"📝 대상날짜: {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"📝 대상날짜: {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"   ㄴ 종료시간(UTC): {end_utc}")
        
        # Datetime to String for BigQuery
        start_utc_str = start_utc.strftime("%Y-%m-%d %H:%M:%S")
        end_utc_str = end_utc.strftime("%Y-%m-%d %H:%M:%S")

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
                    WHERE a.log_date >= '{start_utc_str}'
                      AND a.log_date < '{end_utc_str}'
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
        # 1. 쿼리 실행
        query_job = client.query(query)

        try:
            # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
            # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
            query_job.result()

            # 3. 성공 시 출력
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} pre_joytracking_tracker Batch 완료")
        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
            raise e
    
    print("✅ pre_joytracking_tracker ETL 완료")
    return True



def etl_f_tracker_install(target_date:list, client):

    kst = pytz.timezone('Asia/Seoul')

    for td_str in target_date:
        # [수정 1] 문자열(String)을 datetime 객체로 변환
        # 넘어오는 날짜 형식이 'YYYY-MM-DD'라고 가정합니다.
        try:
            current_date_obj = datetime.strptime(td_str, "%Y-%m-%d")
        except ValueError:
            # 형식이 다를 경우에 대한 예외처리 (예: 시간까지 포함된 경우 등)
            # 필요에 따라 포맷을 수정하세요 ("%Y-%m-%d %H:%M:%S")
            print(f"⚠️ 날짜 형식이 잘못되었습니다: {td_str}")
            continue

        # [수정 2] pytz 라이브러리 사용 시 .replace(tzinfo=...) 보다는 .localize() 권장
        # .replace는 썸머타임이나 역사적 시간대 변경을 제대로 처리 못할 수 있음
        
        # KST 00:00:00 설정 (localize 사용)
        start_kst = kst.localize(current_date_obj)
        
        # KST -> UTC 변환
        start_utc = start_kst.astimezone(pytz.UTC)
        
        # 종료 시간 계산 (하루 뒤)
        end_kst = start_kst + timedelta(days=1)
        end_utc = end_kst.astimezone(pytz.UTC)

        print(f"📝 대상날짜: {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"   ㄴ 종료시간(UTC): {end_utc}")

        # Datetime to String for BigQuery
        start_utc_str = start_utc.strftime("%Y-%m-%d %H:%M:%S")
        end_utc_str = end_utc.strftime("%Y-%m-%d %H:%M:%S")

        # query = f"""
        # MERGE `datahub-478802.datahub.f_tracker_install` as target
        # USING
        # (
        #         WITH TSS AS (
        #         SELECT TRIM(AppID) AS app_id
        #                 , TrackerAccountID AS tracker_account_id
        #                 , TrackerTypeID AS tracker_type_id
        #                 , TRIM(BundleID) AS bundle_id
        #                 , TRIM(Platform) AS platform
        #                 , CountryCode AS country_code
        #                 , TRIM(MediaSource) AS media_source
        #                 , IFNULL(IF(MediaSource = "Organic", "Organic", IF(MediaSource = "Facebook Ads", "FB", IF(MediaSource = "googleadwords_int", "Google", "Other"))), "Other") AS media_source_cat
        #                 , IF(MediaSource = "Organic", "Organic", IF(MediaSource = "GameRoom", "Unknown", "Non-Organic"))  AS is_organic
        #                 , TRIM(Agency) AS agency
        #                 , CASE WHEN b.campaign_name IS NOT NULL THEN b.campaign_name ELSE (IF(a.campaign = '' OR a.campaign is null, "NULL", TRIM(a.campaign))) END AS campaign
        #                 , CASE WHEN MediaSource = 'googleadwords_int' AND LENGTH(a.campaign) <= 11 AND a.campaign NOT LIKE '%UAC%' AND a.campaign NOT LIKE 'PRE_MAIN%' THEN b.campaign_name
        #                         WHEN a.campaign = '' OR a.campaign is null THEN "NULL"
        #                         ELSE TRIM(a.campaign)
        #                 END AS init_campaign
        #                 , TRIM(NORMALIZE(AdsetName, NFC)) AS adset_name
        #                 , TRIM(NORMALIZE(AdName, NFC)) AS ad_name
        #                 , IsRetargeting AS is_retargeting
        #                 , TRIM(AdvertisingID) AS advertising_id
        #                 , TRIM(IDFA) AS idfa
        #                 , TRIM(SiteID) AS site_id
        #                 , TRIM(Channel) AS channel
        #                 , TRIM(CB1MediaSource) AS CB1_media_source
        #                 , TRIM(CB1Campaign) AS CB1_campaign
        #                 , TRIM(CB2MediaSource) AS CB2_media_source
        #                 , TRIM(CB2Campaign) AS CB2_campaign
        #                 , TRIM(CB3MediaSource) AS CB3_media_source
        #                 , TRIM(CB3Campaign) AS CB3_campaign         
        #                 , InstallTime AS install_time
        #                 , EventTime AS event_time
        #                 , EventType AS event_type
        #             FROM (
        #             SELECT app_id                                                                                                                    AS AppID
        #                 , appsflyer_id                                                                                                              AS TrackerAccountID
        #                 , 1                                                                                                                         AS TrackerTypeID     
        #                 , bundle_id                                                                                                                 AS BundleID
        #                 , platform                                                                                                                  AS Platform
        #                 , UPPER(country_code)                                                                                                       AS CountryCode
        #                 -- media_source가 null일 경우 'NULL'로 처리     
        #                 , IFNULL(IF(appsflyer_id = "1000-0000", "GameRoom", IF(media_source = 'organic', "Organic", media_source)), "NULL")         AS MediaSource
        #                 , af_prt                                                                                                                    AS Agency
        #                 , IFNULL(campaign, "NULL")                                                                                                  AS Campaign 
        #                 , af_adset                                                                                                                  AS AdsetName
        #                 , af_ad                                                                                                                     AS AdName
        #                 , is_retargeting                                                                                                            AS IsRetargeting
        #                 , advertising_id                                                                                                            AS AdvertisingID
        #                 , idfa                                                                                                                      AS IDFA
        #                 , af_siteid                                                                                                                 AS SiteID
        #                 , af_channel                                                                                                                AS Channel
        #                 , contributor_1_media_source                                                                                                AS CB1MediaSource
        #                 , contributor_1_campaign                                                                                                    AS CB1Campaign
        #                 , contributor_2_media_source                                                                                                AS CB2MediaSource
        #                 , contributor_2_campaign                                                                                                    AS CB2Campaign
        #                 , contributor_3_media_source                                                                                                AS CB3MediaSource
        #                 , contributor_3_campaign                                                                                                    AS CB3Campaign        
        #                 , install_time                                                                                                              AS InstallTime
        #                 , event_time                                                                                                                AS EventTime
        #                 , event_name                                                                                                                AS EventType
        #             FROM `dataplatform-reporting.AppsflyerLog.V_LogsV2`
        #             WHERE event_time >= '{start_utc_str}' and event_time < '{end_utc_str}' AND
        #                 event_name in ('install', 'reinstall', 're-attribution', 're-engagement')
        #                 AND event_time   >= "2019-12-19 00:48:35.827000 UTC"  
        #             UNION ALL
        #             SELECT app_id                                                                                                                    AS AppID
        #                 , appsflyer_id                                                                                                              AS TrackerAccountID
        #                 , 1                                                                                                                         AS TrackerTypeID
        #                 , bundle_id                                                                                                                 AS BundleID
        #                 , platform                                                                                                                  AS Platform
        #                 , UPPER(country_code)                                                                                                       AS CountryCode
        #                 , IFNULL(media_source, 'Organic')                                                                                           AS MediaSource 
        #                 , af_prt                                                                                                                    AS Agency
        #                 , IFNULL(campaign, "NULL")                                                                                                  AS Campaign
        #                 , af_adset                                                                                                                  AS AdsetName
        #                 , af_ad                                                                                                                     AS AdName  
        #                 , is_retargeting                                                                                                            AS IsRetargeting
        #                 , advertising_id                                                                                                            AS AdvertisingID
        #                 , idfa                                                                                                                      AS IDFA
        #                 , af_siteid                                                                                                                 AS SiteID
        #                 , af_channel                                                                                                                AS Channel
        #                 , contributor_1_media_source                                                                                                AS CB1MediaSource
        #                 , contributor_1_campaign                                                                                                    AS CB1Campaign
        #                 , contributor_2_media_source                                                                                                AS CB2MediaSource
        #                 , contributor_2_campaign                                                                                                    AS CB2Campaign
        #                 , contributor_3_media_source                                                                                                AS CB3MediaSource
        #                 , contributor_3_campaign                                                                                                    AS CB3Campaign       
        #                 , install_time                                                                                                              AS InstallTime 
        #                 , event_time                                                                                                                AS EventTime
        #                 , event_name                                                                                                                AS EventType
        #             FROM `dataplatform-204306.AppsflyerLog.installs_report`
        #             WHERE event_time >= '{start_utc_str}' and event_time < '{end_utc_str}' AND
        #             event_name in ('install', 'reinstall', 're-attribution', 're-engagement')
        #             ) AS a
        #             LEFT JOIN `datahub-478802.datahub.dim_google_campaign` AS b ON a.Campaign = b.campaign_id
        #         )

        #         SELECT TRIM(INFO.app_id) AS app_id
        #             , b.joyple_game_code
        #             , b.market_id
        #             , tracker_account_id
        #             , tracker_type_id
        #             , INFO.bundle_id
        #             , INFO.platform
        #             , INFO.country_code
        #             , INFO.media_source
        #             , INFO.media_source_cat
        #             , INFO.is_organic
        #             , INFO.agency
        #             , INFO.campaign
        #             , INFO.init_campaign
        #             , INFO.adset_name
        #             , INFO.ad_name
        #             , INFO.is_retargeting
        #             , INFO.advertising_id
        #             , INFO.idfa
        #             , INFO.site_id
        #             , INFO.channel
        #             , INFO.CB1_media_source
        #             , INFO.CB1_campaign
        #             , INFO.CB2_media_source
        #             , INFO.CB2_campaign
        #             , INFO.CB3_media_source
        #             , INFO.CB3_campaign
        #             , TIMESTAMP(INFO.install_time) as install_time
        #             , TIMESTAMP(INFO.event_time) as event_time
        #             , INFO.event_type
        #             , EXTRACT(DATE FROM DATETIME(INFO.install_time, "+09:00")) AS install_datekey
        #             FROM (
        #                 SELECT tracker_account_id
        #                     , tracker_type_id
        #                     , ARRAY_AGG(
        #                     STRUCT(
        #                         app_id,
        #                         bundle_id,
        #                         platform,
        #                         country_code,
        #                         media_source,
        #                         media_source_cat,
        #                         is_organic,
        #                         agency,
        #                         campaign,
        #                         init_campaign,
        #                         adset_name,
        #                         ad_name,
        #                         is_retargeting,
        #                         advertising_id,
        #                         idfa,
        #                         site_id,
        #                         channel,
        #                         CB1_media_source,
        #                         CB1_campaign,
        #                         CB2_media_source,
        #                         CB2_campaign,
        #                         CB3_media_source,
        #                         CB3_campaign,                                            
        #                         install_time,
        #                         event_time,
        #                         event_type
        #                     )
        #                     ORDER BY install_time ASC
        #                     LIMIT 1
        #                     )[OFFSET(0)] AS INFO  
        #                 FROM TSS
        #                 WHERE app_id IS NOT NULL
        #                 AND tracker_account_id IS NOT NULL
        #                 AND tracker_account_id <> ""
        #                 AND event_type = "install"
        #                 GROUP BY tracker_account_id, tracker_type_id
        #             ) AS a
        #             LEFT OUTER JOIN `datahub-478802.datahub.dim_app_id` AS b
        #             ON a.INFO.app_id = b.app_id
        # ) as source 
        # ON target.app_id = source.app_id
        # AND target.joyple_game_code = source.joyple_game_code
        # AND target.market_id = source.market_id
        # AND target.tracker_account_id = source.tracker_account_id
        # AND target.tracker_type_id = source.tracker_type_id
        # WHEN NOT MATCHED BY target THEN
        # INSERT (
        #     app_id,
        #     joyple_game_code,
        #     market_id,
        #     tracker_account_id,
        #     tracker_type_id,
        #     bundle_id,
        #     platform,
        #     country_code,
        #     media_source,
        #     media_source_cat,
        #     is_organic,
        #     agency,
        #     campaign,
        #     init_campaign,
        #     adset_name,
        #     ad_name,
        #     is_retargeting,
        #     advertising_id,
        #     idfa,
        #     site_id,
        #     channel,
        #     CB1_media_source,
        #     CB1_campaign,
        #     CB2_media_source,
        #     CB2_campaign,
        #     CB3_media_source,
        #     CB3_campaign,
        #     install_time,
        #     event_time,
        #     event_type,
        #     install_datekey
        #     )
        #     VALUES 
        #     (
        #         source.app_id,
        #         source.joyple_game_code,
        #         source.market_id,
        #         source.tracker_account_id,
        #         source.tracker_type_id,
        #         source.bundle_id,
        #         source.platform,
        #         source.country_code,
        #         source.media_source,
        #         source.media_source_cat,
        #         source.is_organic,
        #         source.agency,
        #         source.campaign,
        #         source.init_campaign,
        #         source.adset_name,
        #         source.ad_name,
        #         source.is_retargeting,
        #         source.advertising_id,
        #         source.idfa,
        #         source.site_id,
        #         source.channel,
        #         source.CB1_media_source,
        #         source.CB1_campaign,
        #         source.CB2_media_source,
        #         source.CB2_campaign,
        #         source.CB3_media_source,
        #         source.CB3_campaign,
        #         source.install_time,
        #         source.event_time,
        #         source.event_type,
        #         source.install_datekey
        #     )
        # WHEN MATCHED THEN
        # UPDATE SET
        #     target.install_time = source.install_time
        #     , target.event_time = source.event_time
        #     , target.event_type = source.event_type
        #     , target.install_datekey = source.install_datekey

        # """

        query = f"""
        MERGE `datahub-478802.datahub.f_tracker_install` as target
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


        # 1. 쿼리 실행
        query_job = client.query(query)

        try:
            # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
            # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
            print(f"📊 처리된 행 개수(Insert/Update): {query_job.num_dml_affected_rows}")

            # 3. 성공 시 출력
            print(f"■ {td_str} f_tracker_install Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
            raise e
    
    print("✅ f_tracker_install ETL 완료")
    return True


def etl_f_tracker_re_engagement(target_date:list, client):

    kst = pytz.timezone('Asia/Seoul')

    for td_str in target_date:
        # [수정 1] 문자열(String)을 datetime 객체로 변환
        # 넘어오는 날짜 형식이 'YYYY-MM-DD'라고 가정합니다.
        try:
            current_date_obj = datetime.strptime(td_str, "%Y-%m-%d")
        except ValueError:
            # 형식이 다를 경우에 대한 예외처리 (예: 시간까지 포함된 경우 등)
            # 필요에 따라 포맷을 수정하세요 ("%Y-%m-%d %H:%M:%S")
            print(f"⚠️ 날짜 형식이 잘못되었습니다: {td_str}")
            continue

        # [수정 2] pytz 라이브러리 사용 시 .replace(tzinfo=...) 보다는 .localize() 권장
        # .replace는 썸머타임이나 역사적 시간대 변경을 제대로 처리 못할 수 있음
        
        # KST 00:00:00 설정 (localize 사용)
        start_kst = kst.localize(current_date_obj)
        
        # KST -> UTC 변환
        start_utc = start_kst.astimezone(pytz.UTC)
        
        # 종료 시간 계산 (하루 뒤)
        end_kst = start_kst + timedelta(days=1)
        end_utc = end_kst.astimezone(pytz.UTC)

        print(f"📝 대상날짜: {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"   ㄴ 종료시간(UTC): {end_utc}")

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
            WHERE event_time >= '{start_utc}' and event_time < '{end_utc}'
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
            WHERE event_time >= '{start_utc}' and event_time < '{end_utc}'
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
            , INFO.install_time as install_time  -- 이미 TIMESTAMP이므로 그대로 사용
            , INFO.event_time as event_time      -- 이미 TIMESTAMP이므로 그대로 사용
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
        # 1. 쿼리 실행
        query_job = client.query(query)

        try:
            # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
            # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
            query_job.result()

            # 3. 성공 시 출력
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} f_tracker_re_engagement Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
            raise e
    
    print("✅ f_tracker_re_engagement ETL 완료")
    return True


def etl_f_cost_campaign_rule(client):

    truncate_query = f"""
        TRUNCATE TABLE `datahub-478802.datahub.f_cost_campaign_rule`
    """

    query = f"""


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
                , game_id 
                , joyple_game_code AS JoypleGameID
                , country
                , currency
                , cost
                , currency_rate
                , cost_currency
                , cmpgn_nm
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
                , optim
                , etc_category
                , os
                , IF(location = 'UK', 'GB', location) AS location -- 계속 유지가 되어야함.
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
                    END AS pre_yn   -- 계속 유지가 되어야함.
                , CASE WHEN game_id = 'KOFS' and country = 'JP' then 1 
                        WHEN game_id = 'RESU' and country IN ('KR', 'TW', 'HK', 'MO', 'ID', 'BN', 'MM', 'MN') then 1
                        ELSE 0 
                        END AS extra_process_required  -- 계속 유지가 되어야함.
                , CASE WHEN media_category in ('Google', 'Google-ACP', 'Google-PC', 'Google-Re')                 THEN 'Google'
                        WHEN media_category in ('Facebook', 'Facebook-3rd', 'Facebook-Gaming', 'Facebook-PC', 'Facebook-Playable', 'Facebook-Re') THEN 'FB'
                        WHEN media_category in ('ADNW','ADNW-Re')                   THEN 'ADNW'
                        WHEN LOWER(gcat) in ('organic','unknown')   THEN 'Organic'
                        ELSE 'Other' 
                END AS media_group  -- 각 빅미디어 모든 매체카테고리 추가
                , target_group 
        FROM (select * except(gcat) ,
                    case when media_category = 'YoutubeBranding' then 'Branding' else gcat -- 계속 유지가 되어야함.
                          end as gcat,
                from (select A.*, B.joyple_game_code
                    from `dataplatform-bdts.mas.v_cost_campaign_rule_group` as a
                    LEFT  JOIN `dataplatform-bdts.mas.game_id` AS B
                    ON A.game_id = B.game_id
                    )   
            )
        ) AS a
        LEFT OUTER JOIN `datahub-478802.datahub.pre_cost_campaign_rule_pre_book` AS c 
        ON (pre_yn = true AND a.JoypleGameID = c.joyple_game_code AND a.cmpgn_dt between campaign_start_date AND campaign_end_date and a.extra_process_required = c.extra_process_required)
    ),
     T_Final 
     AS 
     (
     -- 사전예약이 아닌 원래 데이터들
     SELECT JoypleGameID            AS joyple_game_code
         , upload_time             AS upload_timestamp
         , cmpgn_dt                
         , gcat                    
         , game_id                 
         , country                 AS country_code
         , currency                
         , cost                    
         , cost_currency_uptdt     
         , currency_rate           
         , cost_currency           
         , cmpgn_nm                AS campaign_name
         , cmpgn_id                AS campaign_id
         , adset_nm                AS adset_name
         , adset_id                
         , ad_nm                   AS ad_name
         , ad_id                   AS ad_id
         , impressions             
         , clicks                  
         , mas_cmpgn_yn            
         , creat_dt                AS create_timestamp
         , uptdt_dt                AS update_timestamp
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
         , 'NULL'                  AS pre_cate       
         , media_group             
         , target_group                 
     FROM CostCampaignRule 
     WHERE pre_yn = false
    
     UNION ALL -- 사전예약 전처리 데이터
    
     SELECT a.JoypleGameID          AS joyple_game_code
          , upload_time             AS upload_timestamp
          , CostStartDateInterval   AS cmpgn_dt
          , gcat                    
          , a.game_id               AS game_id
          , country                 AS country_code
          , currency                
          , cost_d                  AS cost
          , costcurrencyuptdt_d     AS cost_currency_uptdt   
          , currency_rate           
          , cost_currency_d         AS cost_currency
          , cmpgn_nm                AS campaign_name
          , cmpgn_id                AS campaign_id
          , adset_nm                AS adset_name
          , adset_id                AS adset_id
          , ad_nm                   AS ad_name
          , ad_id                   AS ad_id
          , impressions             
          , clicks                  
          , mas_cmpgn_yn            
          , creat_dt                AS create_timestamp
          , uptdt_dt                AS update_timestamp
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
          , a.pre_cat               AS pre_cate        
          , media_group             
          , target_group                       
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
         SELECT joyple_game_code, category, cost_start_date, CostStartDateInterval
         FROM `datahub-478802.datahub.pre_cost_campaign_rule_pre_book`
             , unnest(generate_date_array(date(cost_start_date), date_add(date(cost_start_date), interval 6 day),interval 1 day)) AS CostStartDateInterval
     ) AS b 
     ON (a.JoypleGameID = b.joyple_game_code AND a.pre_cat = b.category AND a.pre_date = b.cost_start_date) 
    
     UNION ALL -- 사전예약 캠페인이긴 하나 처리가 안 된 것들
    
     SELECT JoypleGameID          AS joyple_game_code
         , upload_time           AS upload_timestamp
         , cmpgn_dt              
         , gcat                  
         , game_id               
         , country               AS country_code
         , currency              
         , cost                  
         , cost_currency_uptdt   
         , currency_rate         
         , cost_currency         
         , cmpgn_nm              AS campaign_name
         , cmpgn_id              AS campaign_id
         , adset_nm              AS adset_name
         , adset_id              AS adset_id
         , ad_nm                 AS ad_name
         , ad_id                 AS ad_id
         , impressions           
         , clicks                
         , mas_cmpgn_yn          
         , creat_dt              AS create_timestamp
         , uptdt_dt              AS update_timestamp
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
                       , 'NULL'                AS pre_cate          
                       , media_group           
                       , target_group                
     FROM CostCampaignRule 
     WHERE pre_yn = true 
     AND pre_cat is null
     )              
    
     SELECT 
           a.joyple_game_code
         , a.upload_timestamp
         , a.cmpgn_dt
         , a.gcat
         , a.game_id
         , a.country_code
         , a.currency
         , a.cost
         , a.cost_currency_uptdt
         , a.currency_rate
         , a.cost_currency
         , a.campaign_name
         , a.campaign_id
         , a.adset_name
         , a.adset_id
         , a.ad_name
         , a.ad_id
         , a.impressions
         , a.clicks
         , COALESCE(PC.mas_cmpgn_yn, a.mas_cmpgn_yn)           AS mas_cmpgn_yn
         , a.create_timestamp
         , a.update_timestamp
         , a.upload_agent
         , a.user_id
         , COALESCE(PC.media_category, a.media_category)     AS media_category
         , COALESCE(PC.product_category, a.product_category) AS product_category
         , COALESCE(PC.media, a.media)                     AS media
         , COALESCE(PC.media_detail, a.media_detail)         AS media_detail
         , COALESCE(PC.Optim, a.Optim)                     AS optim
         , COALESCE(PC.etc_category, a.etc_category)         AS etc_category
         , COALESCE(PC.os_cam, a.os)                        AS os
         , a.location
         , a.creative_no
         , a.device
         , a.setting_title
         , a.landing_title
         , a.ad_unit
         , a.mediation
         , a.pre_yn
         , a.pre_cate
         , a.media_group
         , a.target_group     
     FROM T_Final as a
     LEFT JOIN (select *
               from `datahub-478802.datahub.dim_pccampaign_list_joytracking`) as PC -- PC캠페인 캠페인 정보 수정
     on A.joyple_game_code = PC.joyple_game_code and A.campaign_name = PC.campaign_name


    """
    # 1. 쿼리 실행
    truncate_query_job = client.query(truncate_query)
    truncate_query_job.result()  # 작업 완료 대기
    query_job = client.query(query)

    try:
        # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
        # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
        query_job.result()

        # 3. 성공 시 출력
        print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")

    except Exception as e:
        # 4. 실패 시 출력
        print(f"❌ 쿼리 실행 중 에러 발생: {e}")
        # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
        raise e
    
    print("✅ f_cost_campaign_rule ETL 완료")