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
    dag_id='ETL_dimension',
    default_args=default_args,
    description='dimension table ETL process to BigQuery',
    schedule= '30 20 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ETL', 'dim', 'bigquery'],
) as dag:
    
    # 빅쿼리 클라이언트 연결
    client = bigquery.Client()



    def etl_f_common_register(target_date:list):

        for td in target_date:
            target_date = td

            # KST 00:00:00 ~ 23:59:59를 UTC로 변환
            start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
            end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)

            # KST 00:00:00 ~ 23:59:59를 UTC로 변환
            query = f"""
                with TA as (
                SELECT a.game_id                                                                                  AS GameID
                    , a.world_id                                                                                 AS WorldID
                    , TRIM(a.server_name)                                                                        AS ServerName
                    , a.joyple_game_code                                                                         AS JoypleGameID
                    , a.auth_method_id                                                                           AS AuthMethodID
                    , TRIM(a.auth_account_name)                                                                  AS AuthAccountName
                    , TRIM(a.game_account_name)                                                                  AS GameAccountName
                    , TRIM(a.game_sub_user_name)                                                                 AS GameSubUserName
                    , a.device_id                                                                                AS DeviceAccountName
                    , a.tracker_account_id                                                                       AS TrackerAccountName
                    , IFNULL(IF(a.mmp_type = 0, 1, a.mmp_type), 1)                                               AS TrackerTypeID
                    , a.market_id                                                                                AS MarketID
                    , a.os_id                                                                                    AS OSID
                    , IFNULL(IF(a.joyple_game_code BETWEEN 60000 AND 60008, 2, a.platform_device_type), 1)       AS PlatformDeviceType
                    , IFNULL(a.game_user_level, 0)                                                               AS AccountLevel
                    , a.ip                                                                                       AS IP
                    , a.access_type_id                                                                           AS AccessTypeID
                    , IFNULL(a.play_seconds, 0)                                                                  AS PlaySeconds
                    , a.log_time                                                                                 AS LogTime
                FROM `dataplatform-204306.CommonLog.Access` AS a
                WHERE a.log_time >= '2025-11-26'
                AND a.log_time < '2025-11-27'
                AND a.access_type_id = 1
                AND a.game_id            IS NOT NULL
                AND a.world_id           IS NOT NULL
                AND a.server_name        IS NOT NULL
                AND a.joyple_game_code   IS NOT NULL
                AND a.auth_method_id     IS NOT NULL
                AND a.auth_account_name  IS NOT NULL
                AND a.auth_account_name  NOT IN ("0", "")
                AND a.game_account_name  IS NOT NULL
                AND a.market_id          IS NOT NULL
                AND a.os_id              IS NOT NULL
                -- AND a.game_user_level    IS NOT NULL
                AND a.ip                 IS NOT NULL
                AND a.access_type_id     IS NOT NULL
                AND a.log_time           IS NOT NULL
                AND CONCAT(CAST(joyple_game_code AS STRING), "|",  auth_method_id , "|", auth_account_name , "|", game_sub_user_name) NOT IN (
                    SELECT UUID FROM `datahub-478802.datahub.f_exclude_game_sub_user_info`
                )
                UNION ALL
                SELECT game_id                                                                                    AS GameID
                    , world_id                                                                                   AS WorldID
                    , TRIM(server_name)                                                                          AS ServerName                                                                            
                    , joyple_game_code                                                                              AS JoypleGameID
                    , auth_method_id                                                                          AS AuthMethodID
                    , TRIM(auth_account_name)                                                                     AS AuthAccountName 
                    , TRIM(game_account_name)                                                                     AS GameAccountName
                    , TRIM(game_sub_user_name)                                                                     AS GameSubUserName
                    , device_account_name                                                                         AS DeviceAccountName
                    , tracker_account_name                                                                       AS TrackerAccountName
                    , IF(tracker_type_id = 0, 1, tracker_type_id)                                                   AS TrackerTypeID
                    , market_id                                                                                  AS MarketID
                    , os_id                                                                                      AS OSID
                    , platform_device_type                                                                        AS PlatformDeviceType
                    , account_level                                                                              AS AccountLevel
                    , ip                                                                                        AS IP
                    , access_type_id                                                                              AS AccessTypeID
                    , play_seconds                                                                               AS PlaySeconds
                    , log_time                                                                                   AS LogTime
                FROM `datahub-478802.datahub.pre_access_log_supplement`
                WHERE CONCAT(CAST(JoypleGameID AS STRING), "|",  AuthMethodID , "|", AuthAccountName , "|", GameSubUserName)  NOT IN (
                    SELECT UUID FROM `datahub-478802.datahub.f_exclude_game_sub_user_info`
                )
                )
                , TB as (
                SELECT 
                            DATE(Info.LogTime, "Asia/Seoul") 			AS reg_date_key
                            , DATETIME(Info.LogTime, "Asia/Seoul")              AS reg_datetime
                            , Info.GameID               AS game_id
                            , Info.WorldID              AS world_id
                            , JoypleGameID              AS joyple_game_code
                            , AuthMethodID              AS auth_method_id
                            , AuthAccountName           AS auth_account_name
                            , Info.TrackerAccountName   AS tracker_account_id
                            , Info.TrackerTypeID        AS tracker_type_id
                            , Info.DeviceAccountName    AS device_id
                            , b.country_code             AS country_code
                            , Info.MarketID             AS market_id
                            , Info.OSID                 AS os_id
                            , Info.PlatformDeviceType   AS platform_device_type
                        
                    FROM (
                        SELECT a.JoypleGameID
                            , a.AuthMethodID
                            , a.AuthAccountName
                            , ARRAY_AGG(STRUCT(a.GameID, a.WorldID, a.TrackerTypeID, a.MarketID, a.OSID, a.IP, a.PlatformDeviceType, a.TrackerAccountName, a.DeviceAccountName, a.LogTime) ORDER BY a.LogTime ASC LIMIT 1)[OFFSET(0)] AS Info
                        FROM (
                            SELECT
                                a.GameID
                                , a.WorldID
                                , a.JoypleGameID
                                , a.AuthMethodID
                                , a.AuthAccountName
                                , a.TrackerAccountName
                                , a.TrackerTypeID 
                                , a.DeviceAccountName
                                , a.PlatformDeviceType
                                , a.MarketID
                                , a.OSID
                                , a.IP
                                , a.LogTime
                            FROM TA AS a
                            WHERE a.AccessTypeID = 1
                            AND a.GameID            IS NOT NULL
                            AND a.WorldID           IS NOT NULL
                            AND a.ServerName        IS NOT NULL
                            AND a.AuthMethodID      IS NOT NULL
                            AND a.AuthAccountName   IS NOT NULL
                            AND a.GameAccountName   IS NOT NULL
                            AND a.MarketID          IS NOT NULL
                            AND a.OSID              IS NOT NULL
                            AND a.AccountLevel      IS NOT NULL
                            AND a.IP                IS NOT NULL
                            AND a.AccessTypeID      IS NOT NULL
                            AND a.LogTime           IS NOT NULL  
                        ) AS a
                        GROUP BY a.JoypleGameID, a.AuthMethodID, a.AuthAccountName
                    ) AS a
                    LEFT OUTER JOIN datahub-478802.datahub.dim_ip4_country_code AS b
                    ON (a.Info.IP = b.ip)
                )

                MERGE `datahub-478802.datahub.f_common_register` AS target
                USING
                (
                    SELECT reg_datekey, reg_datetime, game_id, world_id, joyple_game_code, auth_method_id, auth_account_name, 
                    tracker_account_id, tracker_type_id, device_id, country_code, market_id, os_id, platform_device_type
                    FROM TB
                    WHERE reg_datetime >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                    AND reg_datetime < TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                ) AS source ON target.joyple_game_code = source.joyple_game_code AND target.auth_account_name = source.auth_account_name
                WHEN NOT MATCHED BY target THEN
                INSERT(
                    reg_datekey, reg_datetime, game_id, world_id, joyple_game_code, auth_method_id, auth_account_name, 
                    tracker_account_id, tracker_type_id, device_id, country_code, market_id, os_id, platform_device_type
                    )
                VALUES(
                    source.reg_datekey, source.reg_datetime, source.game_id, source.world_id, source.joyple_game_code, 
                    source.auth_method_id, source.auth_account_name, source.tracker_account_id, source.tracker_type_id, 
                    source.device_id, source.country_code, source.market_id, source.os_id, source.platform_device_type
                    );
                """
            
            client.query(query)
            print(f"■ {target_date.strftime('%Y-%m-%d')} f_common_register Batch 완료")
        
        print("✅ f_common_register ETL 완료")
        return True
    

    def etl_f_common_register_char(target_date:list):

        for td in target_date:
            target_date = td

            # KST 00:00:00 ~ 23:59:59를 UTC로 변환
            start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
            end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)

            query = f"""
            with TA as (
            SELECT a.game_id                                                                                  AS GameID
                , a.world_id                                                                                 AS WorldID
                , TRIM(a.server_name)                                                                        AS ServerName
                , a.joyple_game_code                                                                         AS JoypleGameID
                , a.auth_method_id                                                                           AS AuthMethodID
                , TRIM(a.auth_account_name)                                                                  AS AuthAccountName
                , TRIM(a.game_account_name)                                                                  AS GameAccountName
                , TRIM(a.game_sub_user_name)                                                                 AS GameSubUserName
                , a.device_id                                                                                AS DeviceAccountName
                , a.tracker_account_id                                                                       AS TrackerAccountName
                , IFNULL(IF(a.mmp_type = 0, 1, a.mmp_type), 1)                                               AS TrackerTypeID
                , a.market_id                                                                                AS MarketID
                , a.os_id                                                                                    AS OSID
                , IFNULL(IF(a.joyple_game_code BETWEEN 60000 AND 60008, 2, a.platform_device_type), 1)       AS PlatformDeviceType
                , IFNULL(a.game_user_level, 0)                                                               AS AccountLevel
                , a.ip                                                                                       AS IP
                , a.access_type_id                                                                           AS AccessTypeID
                , IFNULL(a.play_seconds, 0)                                                                  AS PlaySeconds
                , a.log_time                                                                                 AS LogTime
            FROM `dataplatform-204306.CommonLog.Access` AS a
            WHERE a.log_time >= '2025-11-26'
            AND a.log_time < '2025-11-27'
            AND a.access_type_id = 1
            AND a.game_id            IS NOT NULL
            AND a.world_id           IS NOT NULL
            AND a.server_name        IS NOT NULL
            AND a.joyple_game_code   IS NOT NULL
            AND a.auth_method_id     IS NOT NULL
            AND a.auth_account_name  IS NOT NULL
            AND a.auth_account_name  NOT IN ("0", "")
            AND a.game_account_name  IS NOT NULL
            AND a.market_id          IS NOT NULL
            AND a.os_id              IS NOT NULL
            -- AND a.game_user_level    IS NOT NULL
            AND a.ip                 IS NOT NULL
            AND a.access_type_id     IS NOT NULL
            AND a.log_time           IS NOT NULL
            AND CONCAT(CAST(joyple_game_code AS STRING), "|",  auth_method_id , "|", auth_account_name , "|", game_sub_user_name) NOT IN (
                SELECT UUID FROM `dataplatform-reporting.DataService.MV_0181_0000_ExcludedGameSubUserInfoUUID_V`
            )
            UNION ALL
                SELECT game_id                                                                                    AS GameID
                    , world_id                                                                                   AS WorldID
                    , TRIM(server_name)                                                                          AS ServerName                                                                            
                    , joyple_game_code                                                                              AS JoypleGameID
                    , auth_method_id                                                                          AS AuthMethodID
                    , TRIM(auth_account_name)                                                                     AS AuthAccountName 
                    , TRIM(game_account_name)                                                                     AS GameAccountName
                    , TRIM(game_sub_user_name)                                                                     AS GameSubUserName
                    , device_account_name                                                                         AS DeviceAccountName
                    , tracker_account_name                                                                       AS TrackerAccountName
                    , IF(tracker_type_id = 0, 1, tracker_type_id)                                                   AS TrackerTypeID
                    , market_id                                                                                  AS MarketID
                    , os_id                                                                                      AS OSID
                    , platform_device_type                                                                        AS PlatformDeviceType
                    , account_level                                                                              AS AccountLevel
                    , ip                                                                                        AS IP
                    , access_type_id                                                                              AS AccessTypeID
                    , play_seconds                                                                               AS PlaySeconds
                    , log_time                                                                                   AS LogTime
                FROM `datahub-478802.datahub.pre_access_log_supplement`
                WHERE CONCAT(CAST(JoypleGameID AS STRING), "|",  AuthMethodID , "|", AuthAccountName , "|", GameSubUserName)  NOT IN (
                    SELECT UUID FROM `datahub-478802.datahub.f_exclude_game_sub_user_info`
            )
            )
            , TB as (
                SELECT 
                    Info.GameID AS game_id
                    , Info.WorldID AS world_id
                    , JoypleGameID AS joyple_game_code
                    , Info.AuthMethodID AS auth_method_id
                    , Info.AuthAccountName AS auth_account_name
                    , GameAccountName AS game_account_name
                    , GameSubUserName AS game_sub_user_name
                    , Info.ServerName AS server_name
                    , FirstTrackerAccountID AS tracker_account_id
                    , Info.TrackerTypeID AS tracker_type_id
                    , FirstDeviceID AS device_id
                    , b.country_code AS country_code
                    , Info.MarketID AS market_id
                    , Info.OSID AS os_id
                    , Info.PlatformDeviceType AS platform_device_type
                    , DATE(Info.LogTime, "Asia/Seoul") AS game_sub_user_reg_datekey
                    , DATETIME(Info.LogTime, "Asia/Seoul") AS game_sub_user_reg_datetime
                    , c.reg_datekey
                    , c.reg_datetime
                FROM (
                    SELECT a.JoypleGameID
                        , a.GameAccountName
                        , a.GameSubUserName
                        , ARRAY_AGG(STRUCT(a.GameID, a.WorldID, a.ServerName, a.AuthMethodID, a.AuthAccountName, a.TrackerTypeID, a.MarketID, a.OSID, a.IP, a.PlatformDeviceType, a.LogTime) ORDER BY a.LogTime ASC LIMIT 1)[OFFSET(0)] AS Info
                        , ARRAY_AGG(a.TrackerAccountName IGNORE NULLS ORDER BY a.LogTime ASC LIMIT 1)[OFFSET(0)] AS FirstTrackerAccountID
                        , ARRAY_AGG(a.DeviceAccountName IGNORE NULLS ORDER BY a.LogTime ASC LIMIT 1)[OFFSET(0)] AS FirstDeviceID
                    FROM (
                        SELECT a.GameID, a.WorldID, a.JoypleGameID, a.AuthMethodID, a.AuthAccountName,
                            a.TrackerAccountName, a.TrackerTypeID, a.DeviceAccountName, a.GameAccountName,
                            a.GameSubUserName, a.ServerName, a.PlatformDeviceType, a.MarketID, a.OSID, a.IP, a.LogTime
                        FROM TA AS a
                        WHERE a.LogTime >= '2025-11-26' 
                        AND a.LogTime < '2025-11-27'
                        AND a.AccessTypeID = 1
                        AND a.GameID IS NOT NULL
                        AND a.WorldID           IS NOT NULL
                        AND a.ServerName        IS NOT NULL
                        AND a.AuthMethodID      IS NOT NULL
                        AND a.AuthAccountName   IS NOT NULL
                        AND a.GameSubUserName   IS NOT NULL
                        AND a.GameSubUserName   NOT IN ("", "0")
                        AND a.GameAccountName   IS NOT NULL
                        AND a.MarketID          IS NOT NULL
                        AND a.OSID              IS NOT NULL
                        AND a.AccountLevel      IS NOT NULL
                        AND a.IP                IS NOT NULL
                        AND a.AccessTypeID      IS NOT NULL
                        AND a.LogTime           IS NOT NULL  
                    ) AS a
                    GROUP BY a.JoypleGameID, a.GameAccountName, a.GameSubUserName
                ) AS a
                LEFT OUTER JOIN `datahub-478802.datahub.dim_ip4_country_code` AS b ON a.Info.IP = b.ip
                LEFT OUTER JOIN `datahub-478802.datahub.f_common_register` AS c ON a.JoypleGameID = c.joyple_game_code AND CAST(a.Info.AuthAccountName AS STRING) = CAST(c.auth_account_name AS STRING)
            )
            MERGE `datahub-478802.datahub.f_common_register_char` AS target
            USING TB AS source
            ON target.joyple_game_code = source.joyple_game_code 
            AND target.game_sub_user_name = source.game_sub_user_name
            WHEN NOT MATCHED THEN
                INSERT 
                (reg_datekey, reg_datetime, game_id, world_id, joyple_game_code, auth_method_id, auth_account_name, 
                game_account_name, game_sub_user_name, server_name, tracker_account_id, tracker_type_id, device_id, country_code, 
                market_id, os_id, platform_device_type, game_sub_user_reg_datekey, game_sub_user_reg_datetime)
                VALUES
                (source.reg_datekey, source.reg_datetime, source.game_id, source.world_id, source.joyple_game_code, source.auth_method_id, 
                source.auth_account_name, source.game_account_name, source.game_sub_user_name, source.server_name, source.tracker_account_id, 
                source.tracker_type_id, source.device_id, source.country_code, source.market_id, source.os_id, source.platform_device_type, 
                source.game_sub_user_reg_datekey, source.game_sub_user_reg_datetime);
            """

            client.query(query)
            print(f"■ {target_date.strftime('%Y-%m-%d')} f_common_register_char Batch 완료")
        
        print("✅ f_common_register_char ETL 완료")
        return True


    def etl_f_common_access(target_date: list):

        for td in target_date:
            target_date = td

            # KST 00:00:00 ~ 23:59:59를 UTC로 변환
            start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
            end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)

            query = f"""
            with TA as (
            SELECT a.game_id                                                                                  AS GameID
                , a.world_id                                                                                 AS WorldID
                , TRIM(a.server_name)                                                                        AS ServerName
                , a.joyple_game_code                                                                         AS JoypleGameID
                , a.auth_method_id                                                                           AS AuthMethodID
                , TRIM(a.auth_account_name)                                                                  AS AuthAccountName
                , TRIM(a.game_account_name)                                                                  AS GameAccountName
                , TRIM(a.game_sub_user_name)                                                                 AS GameSubUserName
                , a.device_id                                                                                AS DeviceAccountName
                , a.tracker_account_id                                                                       AS TrackerAccountName
                , IFNULL(IF(a.mmp_type = 0, 1, a.mmp_type), 1)                                               AS TrackerTypeID
                , a.market_id                                                                                AS MarketID
                , a.os_id                                                                                    AS OSID
                , IFNULL(IF(a.joyple_game_code BETWEEN 60000 AND 60008, 2, a.platform_device_type), 1)       AS PlatformDeviceType
                , IFNULL(a.game_user_level, 0)                                                               AS AccountLevel
                , a.ip                                                                                       AS IP
                , a.access_type_id                                                                           AS AccessTypeID
                , IFNULL(a.play_seconds, 0)                                                                  AS PlaySeconds
                , a.log_time                                                                                 AS LogTime
            FROM `dataplatform-204306.CommonLog.Access` AS a
            WHERE a.log_time >= '2025-11-26'
            AND a.log_time < '2025-11-27'
            AND a.access_type_id = 1
            AND a.game_id            IS NOT NULL
            AND a.world_id           IS NOT NULL
            AND a.server_name        IS NOT NULL
            AND a.joyple_game_code   IS NOT NULL
            AND a.auth_method_id     IS NOT NULL
            AND a.auth_account_name  IS NOT NULL
            AND a.auth_account_name  NOT IN ("0", "")
            AND a.game_account_name  IS NOT NULL
            AND a.market_id          IS NOT NULL
            AND a.os_id              IS NOT NULL
            -- AND a.game_user_level    IS NOT NULL
            AND a.ip                 IS NOT NULL
            AND a.access_type_id     IS NOT NULL
            AND a.log_time           IS NOT NULL
            AND CONCAT(CAST(joyple_game_code AS STRING), "|",  auth_method_id , "|", auth_account_name , "|", game_sub_user_name) NOT IN (
                SELECT UUID FROM `dataplatform-reporting.DataService.MV_0181_0000_ExcludedGameSubUserInfoUUID_V`
            )
            UNION ALL
            SELECT game_id                                                                                    AS GameID
                , world_id                                                                                   AS WorldID
                , TRIM(server_name)                                                                          AS ServerName                                                                            
                , joyple_game_code                                                                              AS JoypleGameID
                , auth_method_id                                                                          AS AuthMethodID
                , TRIM(auth_account_name)                                                                     AS AuthAccountName 
                , TRIM(game_account_name)                                                                     AS GameAccountName
                , TRIM(game_sub_user_name)                                                                     AS GameSubUserName
                , device_account_name                                                                         AS DeviceAccountName
                , tracker_account_name                                                                       AS TrackerAccountName
                , IF(tracker_type_id = 0, 1, tracker_type_id)                                                   AS TrackerTypeID
                , market_id                                                                                  AS MarketID
                , os_id                                                                                      AS OSID
                , platform_device_type                                                                        AS PlatformDeviceType
                , account_level                                                                              AS AccountLevel
                , ip                                                                                        AS IP
                , access_type_id                                                                              AS AccessTypeID
                , play_seconds                                                                               AS PlaySeconds
                , log_time                                                                                   AS LogTime
            FROM `datahub-478802.datahub.pre_access_log_supplement`
            WHERE CONCAT(CAST(joyple_game_code AS STRING), "|", CAST(auth_method_id AS STRING), "|", auth_account_name, "|", game_sub_user_name)  NOT IN (
                SELECT UUID FROM `dataplatform-reporting.DataService.MV_0181_0000_ExcludedGameSubUserInfoUUID_V`
            )
            )
            , TB as (
                SELECT 
                    DATE(TA.LogTime, "Asia/Seoul") as datekey
                    , c.reg_datekey
                    , DATE_DIFF(DATE(TA.LogTime, "Asia/Seoul"), c.reg_datekey, DAY) reg_datediff	      
                    , TA.GameID AS game_id
                    , TA.WorldID AS world_id
                    , TA.JoypleGameID AS joyple_game_code
                    , TA.GameAccountName AS game_account_name
                    , TA.GameSubUserName AS game_sub_user_name
                    , TA.AuthMethodID AS auth_method_id
                    , TA.AuthAccountName AS auth_account_name
                    , TA.ServerName AS server_name
                    , TA.DeviceAccountName as device_id
                    , b.country_code AS country_code
                    , TA.MarketID AS market_id
                    , TA.OSID AS os_id
                    , TA.PlatformDeviceType AS platform_device_type
                    , TA.AccountLevel AS game_user_level
                    , SUM(CASE WHEN TA.AccessTypeID = 1 THEN 1 ELSE 0 END) AS access_cnt
                    , IF(SUM(TA.PlaySeconds) > 86400, 86400, TA.PlaySeconds) AS play_seconds
                    FROM TA
                    LEFT OUTER JOIN `datahub-478802.datahub.dim_ip4_country_code` AS b ON TA.IP = b.ip
                    LEFT OUTER JOIN `datahub-478802.datahub.f_common_register` AS c ON TA.JoypleGameID = c.joyple_game_code AND CAST(TA.AuthAccountName AS STRING) = CAST(c.auth_account_name AS STRING)
                    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
            )
                MERGE `datahub-478802.datahub.f_common_register_char` AS target
                USING TB AS source
                ON target.datekey = source.datekey
                AND target.game_id = source.game_id
                AND target.world_id = source.world_id
                AND target.joyple_game_code = source.joyple_game_code
                AND target.game_account_name = source.game_account_name
                AND target.game_sub_user_name = source.game_sub_user_name
                AND target.auth_method_id = source.auth_method_id
                AND target.auth_account_name = source.auth_account_name
                AND target.server_name = source.server_name
                AND target.device_id = source.device_id
                AND target.country_code = source.country_code
                AND target.market_id = source.markget_id
                AND target.os_id = source.os_id
                AND target.platform_device_type = source.platform_device_type
                WHEN NOT MATCHED THEN
                INSERT
                (datekey, reg_datekey, reg_datediff, game_id, world_id, joyple_game_code, game_account_name, game_sub_user_name, auth_method_id, auth_account_name,
                server_name, device_id, country_code, market_id, os_id, platform_device_type, game_user_level, access_cnt, play_seconds
                )
                VALUES
                (
                source.datekey, source.reg_datekey, source.reg_datediff, source.game_id, source.world_id, source.joyple_game_code, source.game_account_name, source.game_sub_user_name,
                source.auth_method_id, source.auth_account_name, source.server_name, source.device_id, source.country_code, source.market_id, source.os_id, source.platform_device_type,
                source.game_user_level, source.access_cnt, source.play_secods
                )
            """

            client.query(query)
            print(f"■ {target_date.strftime('%Y-%m-%d')} f_common_access Batch 완료")
        
        print("✅ f_common_access ETL 완료")
        return True
    



            