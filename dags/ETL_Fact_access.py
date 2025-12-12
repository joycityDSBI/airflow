from google.cloud import bigquery
from datetime import datetime, timezone, timedelta
import time
import os
import pytz


# 빅쿼리 클라이언트 연결
client = bigquery.Client()

def etl_f_common_register(target_date:list):

    for td in target_date:
        target_date = td

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        kst = pytz.timezone('Asia/Seoul')
        start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
        end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)
        print(f"📝 시작시간 : ", start_utc, f" 📝 종료시간 : ", end_utc)

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        query = f"""
        MERGE `datahub-478802.datahub.f_common_register` AS target
        USING
        (
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
            WHERE a.log_time >= {start_utc}
            AND a.log_time < {end_utc}
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
            WHERE CONCAT(CAST(joyple_game_code AS STRING), "|",  auth_method_id , "|", auth_account_name , "|", game_sub_user_name)  NOT IN (
                SELECT UUID FROM `datahub-478802.datahub.f_exclude_game_sub_user_info`
            )
            )
            , TB as (
            SELECT 
                        DATE(Info.LogTime, "Asia/Seoul") 			AS reg_datekey
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
            aa.country_code as install_country_code, 
            aa.media_source, 
            aa.media_source_cat, 
            aa.is_organic, 
            aa.agency, 
            aa.campaign, 
            aa.init_campaign,
            aa.adset_name, 
            aa.ad_name, 
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
            aa.install_time, 
            aa.event_time, 
            aa.event_type, 
            aa.install_datekey
            FROM TB 
            LEFT JOIN datahub-478802.datahub.f_tracker_install as aa
            ON TB.tracker_account_id = aa.tracker_account_id AND TB.tracker_type_id = aa.tracker_type_id
            )
            
            SELECT * FROM TC
            
            ) AS source ON target.joyple_game_code = source.joyple_game_code AND target.auth_method_id = source.auth_method_id AND CAST(target.auth_account_name AS STRING) = CAST(source.auth_account_name AS STRING)
            WHEN MATCHED AND (target.campaign <> source.campaign OR target.media_source <> source.media_source)
                        AND source.app_id IS NOT NULL THEN
            UPDATE SET 
                target.app_id = source.app_id
                , target.bundle_id = source.bundle_id
                , target.install_country_code = source.install_country_code
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
                , target.install_time = source.install_time
                , target.event_time = source.event_time
                , target.event_type = source.event_type 
                , target.install_datekey = source.install_datekey

            WHEN NOT MATCHED BY target THEN
            INSERT(
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
            VALUES(
                source.reg_datekey, 
                source.reg_datetime, 
                source.game_id, 
                source.world_id, 
                source.joyple_game_code, 
                source.auth_method_id,
                source.auth_account_name, 
                source.tracker_account_id, 
                source.tracker_type_id, 
                source.device_id, 
                source.reg_country_code,
                source.market_id, 
                source.os_id, 
                source.platform_device_type,
                source.app_id, 
                source.bundle_id, 
                source.install_country_code, 
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
                );
            """
        
        client.query(query)
        print(f"■ {target_date.strftime('%Y-%m-%d')} f_common_register Batch 완료")
    
    print("✅ f_common_register ETL 완료")
    return True

def adjust_f_common_register(target_date:list):

    for td in target_date:
        target_date = td

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        kst = pytz.timezone('Asia/Seoul')
        start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
        end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)
        print(f"📝 시작시간 : ", start_utc, f" 📝 종료시간 : ", end_utc)

        query = f"""
        MERGE datahub-478802.datahub.f_common_register AS target
        USING 
        (
            select DATE(TA.INFO.log_time, "Asia/Seoul") as reg_datekey
            , DATETIME(TA.INFO.log_time, "Asia/Seoul") as reg_datetime
            , TA.INFO.game_id
            , TA.INFO.world_id
            , TA.joyple_game_code
            , TA.auth_method_id
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
            , TC.country_code as install_country_code
            , TB.media_source
            , TB.media_source_cat
            , TB.is_organic
            , TB.agency
            , TB.campaign
            , TB.init_campaign
            , TB.adset_name
            , TB.ad_name
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
            , TB.install_time
            , TB.event_time
            , TB.event_type
            , TB.install_datekey
            from
            (
                select joyple_game_code, auth_method_id, auth_account_name, 
                array_agg(STRUCT(game_id, world_id, auth_method_id, auth_account_name, tracker_account_id, mmp_type, device_id, ip, market_id, os_id, platform_device_type, 
                app_id, log_time) ORDER BY log_time asc)[OFFSET(0)] AS INFO
                from dataplatform-204306.CommonLog.Payment
                where log_time >= {start_utc}
                and log_time < {end_utc}
                group by joyple_game_code, auth_method_id, auth_account_name
            ) TA
            left join 
            datahub-478802.datahub.f_tracker_install as TB
            on TA.INFO.tracker_account_id = TB.tracker_account_id AND TA.INFO.mmp_type = TB.tracker_type_id
            left join 
            datahub-478802.datahub.dim_ip4_country_code as TC
            on TA.INFO.ip = TC.ip
        ) AS source ON target.joyple_game_code = source.joyple_game_code AND target.auth_method_id = source.auth_method_id AND target.auth_account_name = source.auth_account_name
        WHEN NOT MATCHED BY target THEN 
        INSERT
        (
            reg_datekey
            , reg_datetime
            , game_id
            , world_id
            , joyple_game_code
            , auth_method_id
            , auth_account_name
            , tracker_account_id
            , tracker_type_id
            , device_id
            , reg_country_code
            , market_id
            , os_id
            , platform_device_type
            , app_id
            , bundle_id
            , install_country_code
            , media_source
            , media_source_cat
            , is_organic
            , agency
            , campaign
            , init_campaign
            , adset_name
            , ad_name
            , is_retargeting
            , advertising_id
            , idfa
            , site_id
            , channel
            , CB1_media_source
            , CB1_campaign
            , CB2_media_source
            , CB2_campaign
            , CB3_media_source
            , CB3_campaign
            , install_time
            , event_time
            , event_type
            , install_datekey
        )
        VALUES
        (
            source.reg_datekey
            , source.reg_datetime
            , source.game_id
            , source.world_id
            , source.joyple_game_code
            , source.auth_method_id
            , source.auth_account_name
            , source.tracker_account_id
            , source.tracker_type_id
            , source.device_id
            , source.reg_country_code
            , source.market_id
            , source.os_id
            , source.platform_device_type
            , source.app_id
            , source.bundle_id
            , source.install_country_code
            , source.media_source
            , source.media_source_cat
            , source.is_organic
            , source.agency
            , source.campaign
            , source.init_campaign
            , source.adset_name
            , source.ad_name
            , source.is_retargeting
            , source.advertising_id
            , source.idfa
            , source.site_id
            , source.channel
            , source.CB1_media_source
            , source.CB1_campaign
            , source.CB2_media_source
            , source.CB2_campaign
            , source.CB3_media_source
            , source.CB3_campaign
            , source.install_time
            , source.event_time
            , source.event_type
            , source.install_datekey
        )
        """
        client.query(query)
        print(f"■ {target_date.strftime('%Y-%m-%d')} f_common_register_adjust Batch 완료")
    
    print("✅ f_common_register_adjust ETL 완료")
    return True



def etl_f_common_register_char(target_date:list):

    for td in target_date:
        target_date = td

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        kst = pytz.timezone('Asia/Seoul')
        start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
        end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)
        print(f"📝 시작시간 : ", start_utc, f" 📝 종료시간 : ", end_utc)

        query = f"""
        MERGE `datahub-478802.datahub.f_common_register_char` AS target
        USING
        (
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
        WHERE a.log_time >= {start_utc}
        AND a.log_time < {end_utc}
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
                , TRIM(server_name)                                                       AS ServerName                                                                            
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
            WHERE CONCAT(CAST(joyple_game_code AS STRING), "|",  auth_method_id , "|", TRIM(auth_account_name) , "|", TRIM(game_sub_user_name))  NOT IN (
                SELECT UUID FROM `datahub-478802.datahub.f_exclude_game_sub_user_info`
        )
        )
        
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
                    WHERE a.LogTime >= {start_utc}
                    AND a.LogTime < {end_utc}
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
            LEFT OUTER JOIN `datahub-478802.datahub.f_common_register` AS c 
            ON a.JoypleGameID = c.joyple_game_code AND a.AuthMethodID = c.auth_method_id AND CAST(a.Info.AuthAccountName AS STRING) = CAST(c.auth_account_name AS STRING)) AS source
        ON target.joyple_game_code = source.joyple_game_code 
        AND target.game_sub_user_name = source.game_sub_user_name
        WHEN NOT MATCHED THEN
            INSERT 
            (
            reg_datekey
            , reg_datetime
            , game_id
            , world_id
            , joyple_game_code
            , auth_method_id
            , auth_account_name
            , game_sub_user_name
            , server_name
            , tracker_account_id
            , tracker_type_id
            , device_id
            , country_code
            , market_id
            , os_id
            , platform_device_type
            , game_sub_user_reg_datekey
            , game_sub_user_reg_datetime
            )
            VALUES
            (
            source.reg_datekey
            , source.reg_datetime
            , source.game_id
            , source.world_id
            , source.joyple_game_code
            , source.auth_method_id
            , source.auth_account_name
            , source.game_sub_user_name
            , source.server_name
            , source.tracker_account_id
            , source.tracker_type_id
            , source.device_id
            , source.country_code
            , source.market_id
            , source.os_id
            , source.platform_device_type
            , source.game_sub_user_reg_datekey
            , source.game_sub_user_reg_datetime
            );
        """

        client.query(query)
        print(f"■ {target_date.strftime('%Y-%m-%d')} f_common_register_char Batch 완료")
    
    print("✅ f_common_register_char ETL 완료")
    return True


def adjust_f_common_register_char(target_date:list):

    for td in target_date:
        target_date = td

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        kst = pytz.timezone('Asia/Seoul')
        start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
        end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)
        print(f"📝 시작시간 : ", start_utc, f" 📝 종료시간 : ", end_utc)

        query = f"""
        MERGE datahub-478802.datahub.f_common_register_char AS target
        USING 
        (
            select IFNULL(TB.reg_datekey, DATE(TA.INFO.log_time, "Asia/Seoul")) as reg_datekey
            , IFNULL(TB.reg_datetime, DATETIME(TA.INFO.log_time, "Asia/Seoul")) as reg_datetime
            , TA.INFO.game_id
            , TA.INFO.world_id
            , TA.joyple_game_code
            , TA.auth_method_id
            , TA.auth_account_name
            , TA.game_sub_user_name
            , TA.INFO.server_name
            , TA.INFO.tracker_account_id
            , TA.INFO.mmp_type as tracker_type_id
            , TA.INFO.device_id
            , TC.country_code as country_code
            , TA.INFO.market_id
            , TA.INFO.os_id
            , TA.INFO.platform_device_type
            , DATE(TA.INFO.log_time, "Asia/Seoul") as game_sub_user_reg_datekey
            , DATETIME(TA.INFO.log_time, "Asia/Seoul") as game_sub_user_reg_datetime            
            from
            (
                select joyple_game_code, auth_method_id, auth_account_name, game_sub_user_name,
                array_agg(STRUCT(game_id, world_id, auth_method_id, auth_account_name, tracker_account_id, mmp_type, device_id, ip, market_id, os_id, platform_device_type, 
                app_id, log_time, server_name) ORDER BY log_time asc)[OFFSET(0)] AS INFO
                from dataplatform-204306.CommonLog.Payment
                where log_time >= {start_utc}
                and log_time < {end_utc}
                group by joyple_game_code, auth_method_id, auth_account_name, game_sub_user_name
            ) TA
            left join
            datahub-478802.datahub.f_common_register as TB
            on TA.joyple_game_code = TB.joyple_game_code AND TA.auth_method_id = TB.auth_method_id AND CAST(TA.auth_account_name AS STRING) = CAST(TB.auth_account_name AS STRING)
            left join 
            datahub-478802.datahub.dim_ip4_country_code as TC
            on TA.INFO.ip = TC.ip
        ) AS source ON target.joyple_game_code = source.joyple_game_code AND target.auth_method_id = source.auth_method_id AND target.auth_account_name = source.auth_account_name
        WHEN NOT MATCHED BY target THEN 
        INSERT
        (
            reg_datekey
            , reg_datetime
            , game_id
            , world_id
            , joyple_game_code
            , auth_method_id
            , auth_account_name
            , tracker_account_id
            , tracker_type_id
            , device_id
            , reg_country_code
            , market_id
            , os_id
            , platform_device_type
            , game_sub_user_reg_datekey
            , game_sub_user_reg_datetime
        )
        VALUES
        (
            source.reg_datekey
            , source.reg_datetime
            , source.game_id
            , source.world_id
            , source.joyple_game_code
            , source.auth_method_id
            , source.auth_account_name
            , source.tracker_account_id
            , source.tracker_type_id
            , source.device_id
            , source.reg_country_code
            , source.market_id
            , source.os_id
            , source.platform_device_type
            , source.game_sub_user_reg_datekey
            , source.game_sub_user_reg_datetime
        )

        """
        client.query(query)
        print(f"■ {target_date.strftime('%Y-%m-%d')} adjust_common_register_char Batch 완료")
    
    print("✅ adjust_common_register_char ETL 완료")
    return True


def etl_f_common_access(target_date: list):

    for td in target_date:
        target_date = td

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        kst = pytz.timezone('Asia/Seoul')
        start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
        end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)
        print(f"📝 시작시간 : ", start_utc, f" 📝 종료시간 : ", end_utc)

        query = f"""
        MERGE `datahub-478802.datahub.f_common_access` AS target
        USING (
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
        WHERE a.log_time >= {start_utc}
        AND a.log_time < {end_utc}
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
        WHERE CONCAT(CAST(joyple_game_code AS STRING), "|", CAST(auth_method_id AS STRING), "|", auth_account_name, "|", game_sub_user_name)  NOT IN (
            SELECT UUID FROM `datahub-478802.datahub.f_exclude_game_sub_user_info`
        )
        )
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
                , TA.AccessTypeID AS access_type_id
                , SUM(CASE WHEN TA.AccessTypeID = 1 THEN 1 ELSE 0 END) AS access_cnt
                , IF(SUM(TA.PlaySeconds) > 86400, 86400, SUM(TA.PlaySeconds)) AS play_seconds
                FROM TA
                LEFT OUTER JOIN `datahub-478802.datahub.dim_ip4_country_code` AS b ON TA.IP = b.ip
                LEFT OUTER JOIN `datahub-478802.datahub.f_common_register` AS c 
                ON TA.JoypleGameID = c.joyple_game_code AND TA.AuthMethodID = c.auth_method_id AND CAST(TA.AuthAccountName AS STRING) = CAST(c.auth_account_name AS STRING)
                group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
        ) AS source
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
            AND target.market_id = source.market_id
            AND target.os_id = source.os_id
            AND target.platform_device_type = source.platform_device_type
            AND target.game_user_level = source.game_user_level
            AND target.access_type_id = source.access_type_id
            WHEN NOT MATCHED THEN
            INSERT
            (datekey, 
            reg_datekey, 
            reg_datediff, 
            game_id, 
            world_id, 
            joyple_game_code, 
            game_account_name, 
            game_sub_user_name, 
            auth_method_id, 
            auth_account_name,
            server_name, 
            device_id, 
            country_code, 
            market_id, 
            os_id, 
            platform_device_type, 
            game_user_level,
            access_type_id, 
            access_cnt, 
            play_seconds
            )
            VALUES
            (
            source.datekey, 
            source.reg_datekey, 
            source.reg_datediff, 
            source.game_id, 
            source.world_id, 
            source.joyple_game_code, 
            source.game_account_name, 
            source.game_sub_user_name,
            source.auth_method_id, 
            source.auth_account_name, 
            source.server_name, 
            source.device_id, 
            source.country_code, 
            source.market_id, 
            source.os_id, 
            source.platform_device_type,
            source.game_user_level,
            source.access_type_id, 
            source.access_cnt, 
            source.play_seconds
            )
        """

        client.query(query)
        print(f"■ {target_date.strftime('%Y-%m-%d')} f_common_access Batch 완료")
    
    print("✅ f_common_access ETL 완료")
    return True
    



            