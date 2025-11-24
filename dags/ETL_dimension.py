# Airflow function
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.models import Variable

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
    dag_id='ETL_dimension',
    default_args=default_args,
    description='dimension table ETL process to BigQuery',
    schedule= '30 20 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ETL', 'dim', 'bigquery'],
) as dag:
    
    client = bigquery.Client()

    def etl_dim_os():

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
        end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)

        # ETL 작업 수행
        query = f"""
        MERGE `datahub-478802.datahub.dim_os` T
        USING (
            SELECT
                DISTINCT
                os_id, null as os_name, null as os_name_lower
            FROM `dataplatform-204306.CommonLog.Access`
            where log_time >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
            AND log_time < TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
        ) S
        on T.os_id = S.os_id
        WHEN MATCHED THEN
        UPDATE SET
            T.os_name = COALESCE(S.os_name, T.os_name),
            T.os_name_lower = COALESCE(S.os_name_lower, T.os_name_lower)
            T.create_timestamp = COALESCE(CURRENT_TIMESTAMP(), T.create_timestamp)
        WHEN NOT MATCHED THEN 
        INSERT (os_id, os_name, os_name_lower, create_timestamp)
        VALUES (S.os_id, null, null, CURRENT_TIMESTAMP())
        """

        client.query(query)
        print("✅ dim_os ETL 완료")
        return True
    

    def etl_dim_AFC_campaign():

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
        end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)

        truncate_query = f"""
            TRUNCATE TABLE `datahub-478802.datahub.dim_AFC_campaign'
        """

        query = f"""
        INSERT INTO `datahub-478802.datahub.dim_AFC_campaign'
        (app_id, media_source, init_campaign, Gcat, uptdt_campaign, media_category, product_category, media, media_detail, optim, etc_category, os_cam, geo_cam, date_cam, 
        creative_no, device, setting_title, landing_title, ad_unit, mediation, create_YN, update_YN, rule_YN, class, target_group, media_group, country_group, create_timestamp)
        SELECT DISTINCT
            app_id                                  AS AppID
            , NORMALIZE(media_source, NFC)            AS MediaSource
            , NORMALIZE(init_campaign, NFC)           AS InitCampaign
            , gcat                                    AS GCat
            , NORMALIZE(uptdt_campaign, NFC)          AS UptdtCampaign
            , media_category                          AS MediaCategory
            , product_category                        AS ProductCategory
            , media                                   AS Media
            , media_detail                            AS MediaDetail
            , optim                                   AS Optim
            , etc_category                            AS EtcCategory
            , os_cam                                  AS OSCam
            , geo_cam                                 AS GEOCam
            , date_cam                                AS DateCam
            , creative_no                             AS CreativeNo
            , device                                  AS Device
            , setting_title                           AS SettingTitle
            , landing_title                           AS LandingTitle
            , ad_unit                                 AS ADUnit
            , mediation                               AS Mediation
            , create_yn                               AS CreateYN
            , update_yn                               AS UpdateYN
            , rule_yn                                 AS RuleYN
            , class                                   AS Class
            , CASE
                WHEN 
                    etc_category = 'L&F' THEN '6.ETC'  
                WHEN 
                    (media_category IN ('ADNW','AppleSA.Self') AND media NOT IN ('Mistplay') AND class LIKE '%tROAS%') OR
                    (media_category IN ('Facebook' ,'Facebook-Gaming','Facebook-Playable','Facebook-Re' ,'Mytarget.Self') AND class in('VO','VO(MinROAS)') ) OR
                    (media_category IN ('Google','Google-Re') AND etc_category NOT IN ('GoldAccel') AND class LIKE '%tROAS%') OR
                    (media_category IN ('Facebook' ,'Facebook-Gaming','Facebook-Playable','Facebook-Re' ,'Mytarget.Self', 'Google', 'Google-Re') AND class IN ('CEO(HVU)','GFS(HVU)') ) THEN '1.UA-HVU'
                WHEN 
                    (media_category IN ('ADNW','AppleSA.Self') AND (class LIKE '%tRET%' OR class LIKE '%tROAS%' OR class IN('CPA','CPC','CPM', 'CPC_Retargeting', 'CPM_AEO(Level)','CPM_AEO(Purchase)','CPM_CEO(Pecan)' )) ) OR 
                    (media_category in( 'Facebook' ,'Facebook-Gaming','Facebook-Playable','Facebook-Re' ,'Mytarget.Self') AND class in('AEO(Level)','AEO','AEO(Gem)','CEO','CEO(Pecan)','CEO(Model)','CEO(Action)') ) OR
                    (media_category IN ('Google','Google-Re') AND (class in('tCPA_ETC','tCPA_Purchase','tCPA','tCPA(GFS)_ETC') OR class LIKE '%tROAS%')) OR
                    (media_category IN ( 'Facebook' ,'Facebook-Gaming','Facebook-Playable','Facebook-Re' ,'Mytarget.Self','Google', 'Google-Re') AND class IN ('CEO(VU)','GFS(VU)') ) THEN '2.UA-VU'
                WHEN 
                    (media_category IN ('ADNW','AppleSA.Self' )  AND (class IN ('nCPI','nCPA') OR class LIKE '%tCPI%')) OR
                    (media_category IN ('Facebook' ,'Facebook-Gaming','Facebook-Playable','Facebook-Re' ,'Mytarget.Self') AND class in('MAIA','MAIE' ,'Linkclick','NONE') ) OR
                    (media_category IN ('Google','Google-Re') AND class in('tCPI_ETC','tCPI','tCPI-Adv_ETC','tCPI-Adv_Purchase','NONE','ARO') ) OR 
                    (media_category IN ('Facebook' ,'Facebook-Gaming','Facebook-Playable','Facebook-Re' ,'Mytarget.Self','Google', 'Google-Re')) AND class IN ('CEO(Install)','GFS(Install)') THEN '3.UA-Install'
                WHEN 
                    media_category = 'Restricted' THEN '4.UA-Restricted'
                WHEN 
                    gcat IN ('Organic','Unknown') THEN '5.Organic'
                ELSE '6.ETC'
            END                                      AS TargetGroup
            , CASE WHEN media_category = 'Google'                 THEN 'Google'
                    WHEN media_category = 'Facebook'               THEN 'FB'
                    WHEN media_category = 'ADNW'                   THEN 'ADNW'
                    WHEN LOWER(gcat) IN ('organic','unknown')      THEN 'Organic'
                    ELSE 'Other' 
            END                                     AS MediaGroup
            , upload_time                              AS UploadTimestamp  
        FROM (
        SELECT app_id
            , CASE WHEN LOWER(TRIM(media_source)) = 'organic'   THEN 'Organic'
                    ELSE gcat
                END AS gcat
            , media_category
            , product_category
            , media
            , media_detail
            , CASE WHEN LOWER(TRIM(media_source)) = 'organic'   THEN 'Organic'
                    ELSE TRIM(media_source) 
                END AS media_source
            , optim
            , etc_category
            , os                                  AS os_cam
            , IF(location = 'UK', 'GB', location) AS geo_cam
            , cmpgn_dt                            AS date_cam
            , creative_no 
            , device
            , setting_title
            , landing_title
            , ad_unit
            , mediation
            , create_yn
            , update_yn
            , rule_yn
            , init_campaign
            , uptdt_campaign
            , CASE
                    WHEN etc_category   = 'L&F'                                                                              THEN CONCAT(optim,'_L&F')
                    WHEN media_category = 'ADNW' AND (optim IS NULL OR optim = '구분없음')                                    THEN product_category
                    WHEN media_category = 'ADNW'                                                                             THEN CONCAT(product_category,'_',optim)
                    WHEN media_category IN ('Facebook' ,'Facebook-Gaming','Facebook-Playable','Facebook-Re')                 THEN optim        
                    WHEN media_category = 'AppleSA.Self'                                                                     THEN product_category
                    WHEN media_category = 'Mytarget.Self'                                                                    THEN optim
                    WHEN media_category IN ('Google','Google-Re') AND (etc_category IS NULL OR etc_category = '구분없음')     THEN optim
                    WHEN media_category IN ('Google','Google-Re') AND etc_category =  'Purchase'                             THEN CONCAT(optim,'_',etc_category)
                    WHEN media_category IN ('Google','Google-Re') AND etc_category != 'Purchase'                             THEN CONCAT(optim,'_ETC')
                    ELSE '구분없음'
                END AS class
            , upload_time       
        FROM `dataplatform-bdts.mas.af_campaign_rule`
        where 
        ) AS a
        """

        client.query(truncate_query)
        client.query(query)
        print("✅ dim_AFC_campaign ETL 완료")
        
        return True
    

    def etl_dim_auth_type():

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
        end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)

        query = f"""
        
        """


        

