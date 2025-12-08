# Airflow function
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.models.baseoperator import chain

import pandas as pd
from google.cloud import bigquery
from google.auth.transport.requests import Request
import logging

from datetime import datetime, timezone, timedelta
import time
import os
import pytz



# 방법 1: 현재 KST 날짜 기준
target_date_kst = datetime.now(pytz.timezone('Asia/Seoul')).date()  # 2025-11-24
kst = pytz.timezone('Asia/Seoul')

# target_date_kst를 datetime으로 변환
target_date = datetime.combine(target_date_kst, datetime.min.time())  # 2025-11-24 00:00:00
print(f"Target KST Date: {target_date_kst}")
target_date = [target_date]

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
    dag_id='ETL_Fact_tracker',
    default_args=default_args,
    description='tracker(Appsflyer) table ETL process to BigQuery',
    schedule= '30 20 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ETL', 'fact', 'bigquery'],
) as dag:
    
    # 빅쿼리 클라이언트 연결
    client = bigquery.Client()

    def etl_pre_tracker_install():

        truncate_query = f"""
        TRUNCATE TABLE `datahub-478802.datahub.pre_tracker_install`
        """

        query = f"""
        INSERT INTO `datahub-478802.datahub.pre_tracker_install` 
        (app_id,
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
        event_type
        )

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
            , appsflyer_device_id                                                                                                       AS TrackerAccountID
            , 1                                                                                                                         AS TrackerTypeID  -- mmp_type
            , bundle_id                                                                                                                 AS BundleID
            , platform                                                                                                                  AS Platform
            , UPPER(country_code)                                                                                                       AS CountryCode
            -- media_source가 null일 경우 'NULL'로 
            , IFNULL(IF(appsflyer_device_id = "1000-0000", "GameRoom", IF(media_source = 'organic', "Organic", media_source)), "NULL")  AS MediaSource
            , agency                                                                                                                    AS Agency
            , IFNULL(CASE WHEN fb_campaign_name IS NOT NULL THEN fb_campaign_name ELSE campaign END, "NULL")                            AS Campaign
            , CASE WHEN fb_adset_name IS NOT NULL THEN fb_adset_name ELSE af_adset END                                                  AS AdsetName
            , CASE WHEN fb_adgroup_name IS NOT NULL THEN fb_adgroup_name ELSE af_ad END                                                 AS AdName
            , is_retargeting                                                                                                            AS IsRetargeting
            , advertising_id                                                                                                            AS AdvertisingID
            , idfa                                                                                                                      AS IDFA
            , af_siteid                                                                                                                 AS SiteID
            , af_channel                                                                                                                AS Channel
            , "NULL"                                                                                                                    AS CB1MediaSource
            , "NULL"                                                                                                                    AS CB1Campaign
            , "NULL"                                                                                                                    AS CB2MediaSource
            , "NULL"                                                                                                                    AS CB2Campaign
            , "NULL"                                                                                                                    AS CB3MediaSource
            , "NULL"                                                                                                                    AS CB3Campaign
            , install_time                                                                                                              AS InstallTime
            , event_time                                                                                                                AS EventTime
            , event_type                                                                                                                AS EventType
        FROM `dataplatform-reporting.AppsflyerLog.V_LogsV1` AS a
        WHERE event_type in ('install', 'reinstall', 're-attribution', 're-engagement')
            -- AND download_time < TIMESTAMP_ADD(install_time, INTERVAL 1 DAY)
            -- AND install_time <= "2020-06-17 05:47:38 UTC"  
            AND event_time   <= "2020-06-17 05:47:38 UTC" 
        UNION ALL
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
        WHERE event_name in ('install', 'reinstall', 're-attribution', 're-engagement')
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
        WHERE event_name in ('install', 'reinstall', 're-attribution', 're-engagement')
        ) AS a
        LEFT JOIN `datahub-478802.datahub.dim_google_campaign` AS b ON a.Campaign = b.campaign_id
        """

        client.query(truncate_query)
        time.sleep(5)
        client.query(query)
        
        print("✅ pre_payment_info_fix ETL 완료")


    def etl_f_tracker_install():

        truncate_query = f"""
        TRUNCATE TABLE `datahub-478802.datahub.f_tracker_install`
        """

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
            FROM `datahub-478802.datahub.pre_tracker_install`
            WHERE app_id IS NOT NULL
            AND tracker_account_id IS NOT NULL
            AND tracker_account_id <> ""
            AND event_type = "install"
            GROUP BY tracker_account_id, tracker_type_id
        ) AS a
        LEFT OUTER JOIN `datahub-478802.datahub.dim_app_id` AS b
        ON a.INFO.app_id = b.app_id
        """
        client.query(truncate_query)
        time.sleep(5)
        client.query(query)

        print("✅ f_tracker_install ETL 완료")


    # def etl_f_trakcer_install(target_date: list):
    #     ################################################################### truncate / insert로 처리하여 변경

    #     for td in target_date:
    #         target_date = td

    #         # KST
    #         start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
    #         end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)

    #         query=f"""
    #         MERGE `datahub-478802.datahub.f_tracker_install` AS target
    #             USING
    #             (
    #                 SELECT TRIM(INFO.app_id) AS app_id
    #                     , b.joyple_game_code
    #                     , b.market_id
    #                     , tracker_account_id
    #                     , tracker_type_id
    #                     , INFO.bundle_id
    #                     , INFO.platform
    #                     , INFO.country_code
    #                     , INFO.media_source
    #                     , INFO.media_source_cat
    #                     , INFO.is_organic
    #                     , INFO.agency
    #                     , INFO.campaign
    #                     , INFO.init_campaign
    #                     , INFO.adset_name
    #                     , INFO.ad_name
    #                     , INFO.is_retargeting
    #                     , INFO.advertising_id
    #                     , INFO.idfa
    #                     , INFO.site_id
    #                     , INFO.channel
    #                     , INFO.CB1_media_source
    #                     , INFO.CB1_campaign
    #                     , INFO.CB2_media_source
    #                     , INFO.CB2_campaign
    #                     , INFO.CB3_media_source
    #                     , INFO.CB3_campaign
    #                     , INFO.install_time
    #                     , INFO.event_time
    #                     , INFO.event_type
    #                     , EXTRACT(DATE FROM DATETIME(INFO.install_time, "+09:00")) AS install_datekey
    #                 FROM (
    #                     SELECT tracker_account_id
    #                         , tracker_type_id
    #                         , ARRAY_AGG(
    #                         STRUCT(
    #                             app_id,
    #                             bundle_id,
    #                             platform,
    #                             country_code,
    #                             media_source,
    #                             media_source_cat,
    #                             is_organic,
    #                             agency,
    #                             campaign,
    #                             init_campaign,
    #                             adset_name,
    #                             ad_name,
    #                             is_retargeting,
    #                             advertising_id,
    #                             idfa,
    #                             site_id,
    #                             channel,
    #                             CB1_media_source,
    #                             CB1_campaign,
    #                             CB2_media_source,
    #                             CB2_campaign,
    #                             CB3_media_source,
    #                             CB3_campaign,                                            
    #                             install_time,
    #                             event_time,
    #                             event_type
    #                         )
    #                         ORDER BY install_time ASC
    #                         LIMIT 1
    #                         )[OFFSET(0)] AS INFO  
    #                     FROM `datahub-478802.datahub.pre_tracker_install`
    #                     WHERE event_time >= '2025-11-25'
    #                     AND event_time <  '2025-11-26'
    #                     AND app_id IS NOT NULL
    #                     AND tracker_account_id IS NOT NULL
    #                     AND tracker_account_id <> ""
    #                     AND event_type = "install"
    #                     GROUP BY tracker_account_id, tracker_type_id
    #                 ) AS a
    #                 LEFT OUTER JOIN `datahub-478802.datahub.dim_app_id` AS b
    #                 ON a.INFO.app_id = b.app_id
    #             ) AS source ON target.tracker_account_id = source.tracker_account_id AND target.tracker_type_id = source.tracker_type_id
    #             WHEN MATCHED AND ((target.app_id <> source.app_id) OR (target.market_id <> source.market_id)) THEN
    #             UPDATE SET target.app_id           =  source.app_id
    #                     , target.joyple_game_code    =  source.joyple_game_code
    #                     , target.market_id        =  source.market_id
    #                     , target.bundle_id        =  source.bundle_id
    #                     , target.platform        =  source.platform
    #                     , target.country_code     =  source.country_code
    #                     , target.media_source     =  source.media_source
    #                     , target.media_source_cat  =  source.media_source_cat
    #                     , target.is_organic       =  source.is_organic
    #                     , target.agency          =  source.agency
    #                     , target.campaign        =  source.campaign
    #                     , target.init_campaign    =  source.init_campaign
    #                     , target.adset_name       =  source.adset_name
    #                     , target.ad_name          =  source.ad_name
    #                     , target.is_retargeting   =  source.is_retargeting
    #                     , target.advertising_id   =  source.advertising_id
    #                     , target.idfa            =  source.idfa
    #                     , target.site_id          =  source.site_id
    #                     , target.channel         =  source.channel
    #                     , target.CB1_media_source  =  source.CB1_media_source
    #                     , target.CB1_campaign     =  source.CB1_campaign
    #                     , target.CB2_media_source  =  source.CB2_media_source
    #                     , target.CB2_campaign     =  source.CB2_campaign
    #                     , target.CB3_media_source  =  source.CB3_media_source
    #                     , target.CB3_campaign     =  source.CB3_campaign
    #                     , target.install_time = source.install_time
    #                     , target.event_time = source.event_time
    #                     , target.event_type = source.event_type
    #                     , target.install_datekey = source.install_datekey
    #             WHEN NOT MATCHED BY target THEN
    #             INSERT (
    #                     app_id
    #                 , joyple_game_code
    #                 , market_id
    #                 , tracker_account_id
    #                 , tracker_type_id
    #                 , bundle_id
    #                 , platform
    #                 , country_code
    #                 , media_source
    #                 , media_source_cat
    #                 , is_organic
    #                 , agency
    #                 , campaign
    #                 , init_campaign
    #                 , adset_name
    #                 , ad_name
    #                 , is_retargeting
    #                 , advertising_id
    #                 , idfa
    #                 , site_id
    #                 , channel
    #                 , CB1_media_source
    #                 , CB1_campaign
    #                 , CB2_media_source
    #                 , CB2_campaign
    #                 , CB3_media_source
    #                 , CB3_campaign
    #                 , install_time
    #                 , event_time
    #                 , event_type
    #                 , install_datekey
    #             )
    #             VALUES (
    #                     source.app_id
    #                     , source.joyple_game_code
    #                     , source.market_id  
    #                     , source.tracker_account_id
    #                     , source.tracker_type_id
    #                     , source.bundle_id
    #                     , source.platform
    #                     , source.country_code
    #                     , source.media_source
    #                     , source.media_source_cat
    #                     , source.is_organic
    #                     , source.agency
    #                     , source.campaign
    #                     , source.init_campaign
    #                     , source.adset_name
    #                     , source.ad_name
    #                     , source.is_retargeting
    #                     , source.advertising_id
    #                     , source.idfa
    #                     , source.site_id
    #                     , source.channel
    #                     , source.CB1_media_source
    #                     , source.CB1_campaign
    #                     , source.CB2_media_source
    #                     , source.CB2_campaign
    #                     , source.CB3_media_source
    #                     , source.CB3_campaign
    #                     , source.install_time
    #                     , source.event_time
    #                     , source.event_type
    #                     , source.install_datekey
    #             )

    #             """
            
    #         client.query(query)
    #         print(f"■ {target_date.strftime('%Y-%m-%d')} f_tracker_install Batch 완료")
        
    #     print("✅ f_tracker_install ETL 완료")
    #     return True
    

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
        , class
        , media_group
        , target_group  
        )

        WITH CostCampaignRule
        AS (
        SELECT a.*
                , Category                                                        AS pre_cat
                , campaign_start_date                                               AS cmpgn_start
                , campaign_end_date                                                AS cmpgn_end
                , cost_start_date                                                   AS pre_date
                , CASE WHEN game_id = 'KOFS' AND country = 'JP' THEN 1 ELSE 0 END AS extra_process_required
                , CASE WHEN Etc_Category = 'L&F' THEN '6.ETC'
                    WHEN 
                    (Media_Category IN ('ADNW','AppleSA.Self') AND media NOT IN ('Mistplay') AND Class LIKE '%tROAS%') OR
                    (Media_Category IN ('Facebook' ,'Facebook-Gaming','Facebook-Playable','Facebook-Re' ,'Mytarget.Self') AND Class in('VO','VO(MinROAS)') ) OR
                    (Media_Category IN ('Google','Google-Re') AND Etc_Category NOT IN ('GoldAccel') AND Class LIKE '%tROAS%') OR  -- MediaCategory = 'Google'에서 MediaCategory IN ('Google','Google-Re')으로 변경
                    (Media_Category IN ('Facebook' ,'Facebook-Gaming','Facebook-Playable','Facebook-Re' ,'Mytarget.Self', 'Google', 'Google-Re') AND Class IN ('CEO(HVU)','GFS(HVU)') ) THEN '1.UA-HVU'
                    WHEN 
                    (Media_Category IN ('ADNW','AppleSA.Self') AND (Class LIKE '%tRET%' OR Class LIKE '%tROAS%' OR Class  in('CPA','CPC','CPM', 'CPC_Retargeting', 'CPM_AEO(Level)','CPM_AEO(Purchase)','CPM_CEO(Pecan)' )) ) OR 
                    (Media_Category in( 'Facebook' ,'Facebook-Gaming','Facebook-Playable','Facebook-Re' ,'Mytarget.Self') AND Class in('AEO(Level)','AEO','AEO(Gem)','CEO','CEO(Pecan)','CEO(Model)','CEO(Action)') ) OR
                    (Media_Category IN ('Google','Google-Re') AND (Class in('tCPA_ETC','tCPA_Purchase','tCPA','tCPA(GFS)_ETC') OR Class LIKE '%tROAS%')) OR  -- MediaCategory = 'Google'에서 MediaCategory IN ('Google','Google-Re')으로 변경
                    (Media_Category IN ( 'Facebook' ,'Facebook-Gaming','Facebook-Playable','Facebook-Re' ,'Mytarget.Self','Google', 'Google-Re') AND Class IN ('CEO(VU)','GFS(VU)') ) THEN '2.UA-VU'        
                    WHEN 
                    (Media_Category IN ('ADNW','AppleSA.Self' )  AND (Class IN ('nCPI','nCPA') OR Class LIKE '%tCPI%')) OR
                    (Media_Category IN ('Facebook' ,'Facebook-Gaming','Facebook-Playable','Facebook-Re' ,'Mytarget.Self') AND Class in('MAIA','MAIE' ,'Linkclick','NONE') ) OR
                    (Media_Category IN ('Google','Google-Re') AND Class in('tCPI_ETC','tCPI','tCPI-Adv_ETC','tCPI-Adv_Purchase','NONE','ARO') ) OR   -- MediaCategory = 'Google'에서 MediaCategory IN ('Google','Google-Re')으로 변경
                    (Media_Category IN ('Facebook' ,'Facebook-Gaming','Facebook-Playable','Facebook-Re' ,'Mytarget.Self','Google', 'Google-Re')) AND Class IN ('CEO(Install)','GFS(Install)') THEN '3.UA-Install'
                    WHEN Media_Category = 'Restricted' THEN '4.UA-Restricted'
                    ELSE '6.ETC'
                END AS Target_Group ### 기존 작성되어있던 Targetgroup 쿼리에 Google-Re 캠페인, L&F 캠페인 처리 추가
                
                , CASE WHEN cmpgn_nm LIKE '%Credit%' THEN 0
                    ELSE cost_currency 
                END AS cost_currency_uptdt ## 기존컬럼인 CostUptdt 삭제 후 -> 삭제해도 다른테이블에 영향 없음 
                                            ## Cost_currency 테이블에 Credit 캠페인 비용을 0 으로 처리한
                                            ## CostCurrencyUptdt 컬럼 생성
        FROM (SELECT upload_time
                    , cmpgn_dt
                    , gcat
                    , CASE WHEN game_id in ('MTSG','DS') THEN 'DS' 
                            when game_id in ('JT', 'JTWN') Then 'JTWN'
                            ELSE game_id end as game_id -- 게임 약어 혼용으로 인한 변경 처리, MAS 시스템에서 게임명 입력을 누락한 케이스
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
                        else cmpgn_nm end as cmpgn_nm ## 기존 CostUptdt 있던 하드코딩을 cmpgn_nm에서 하드코딩하는 것으로 변경
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
                    , IF(location = 'UK', 'GB', location) AS location
                    , creative_no
                    , device
                    , setting_title
                    , landing_title
                    , ad_unit
                    , mediation
                    , impressions ## 기존 데사실 Cost 테이블에 있지만 없는 컬럼 추가 
                    , clicks ## ## 기존 데사실 Cost 테이블에 있지만 없는 컬럼 추가 
                    , CASE WHEN media_category LIKE '%-Pre'   THEN true
                            WHEN media_category LIKE '%-Pre-%' THEN true
                            WHEN media_category LIKE 'Pre-%'   THEN true
                            WHEN media_category LIKE 'pre-%'   THEN true
                            WHEN media_category LIKE '%-pre'   THEN true
                            WHEN media_category IN ('Preregister','Update-Preregister','Update -Preregister','Google-ACP') THEN true
                            WHEN media IN ('GL-PC-UpdatePre','GL-PC-Pre','FB-PC-UpdatePre','FB-PC-Pre') THEN true  -- 210923 수정한 부분
                            ELSE false 
                        END AS pre_yn
                    , CASE WHEN game_id = 'KOFS' and country = 'JP' then 1 
                            WHEN game_id = 'RESU' and country = 'KR' then 1
                            ELSE 0 
                            END AS extra_process_required ## 기존 작성되어있던 extra_process_required 쿼리에 RESU 캠페인 처리 추가   
                    ,CASE
                        WHEN etc_category = 'L&F' THEN CONCAT(optim,'_L&F')
                        WHEN media_category = 'ADNW' AND (optim IS NULL OR optim = '구분없음') THEN product_category
                        WHEN media_category = 'ADNW' THEN CONCAT(product_category,'_',optim)
                        WHEN media_category in( 'Facebook' ,'Facebook-Gaming','Facebook-Playable','Facebook-Re') THEN optim2        
                        WHEN media_category = 'AppleSA.Self'   THEN product_category
                        WHEN media_category = 'Mytarget.Self'   THEN optim
                        WHEN media_category in ('Google','Google-Re') AND (etc_category IS NULL OR etc_category = '구분없음') THEN optim
                        WHEN media_category in ('Google','Google-Re') AND etc_category = 'Purchase' THEN CONCAT(optim,'_',etc_category)
                        WHEN media_category in ('Google','Google-Re') AND etc_category != 'Purchase' THEN CONCAT(optim,'_ETC')
                    ELSE '구분없음'
                    END AS class
                    , CASE WHEN media_category = 'Google'                 THEN 'Google'
                        WHEN media_category = 'Facebook'               THEN 'FB'
                        WHEN media_category = 'ADNW'                   THEN 'ADNW'
                        WHEN LOWER(gcat) in ('organic','unknown')   THEN 'Organic'
                        ELSE 'Other' 
                    END AS media_group
            FROM (select * ,
                        case when optim  = 'NONE' and adset_nm like '%MAIA%' then 'MAIA'
                            when  optim  = 'NONE' and   adset_nm like '%AEO%' then 'AEO'
                            when  optim  = 'NONE' and  adset_nm like '%VO%' then 'VO'
                        else optim end as optim2 ## optim 'NONE' 값인 Case 수정 로직 추가
                from `dataplatform-bdts.mas.cost_campaign_rule_game`) AS a
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
            , class                   
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
            , class                   
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
                , class                   
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
                    , class
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
            , class                 
            , media_group           
            , target_group                
        FROM CostCampaignRule 
        WHERE pre_yn = true 
            AND pre_cat is null
        )

        SELECT joyple_game_code
            , upload_timestamp
            , cmpgn_dt
            , gcat
            , game_id
            , IF(country_code = 'UK', 'GB', country_code) AS country_code
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
            , class
            , media_group
            , target_group     
        FROM T_Final
        """

        client.query(truncate_query)
        time.sleep(5)
        client.query(query)

        print("✅ f_cost_campaign_rule ETL 완료")

