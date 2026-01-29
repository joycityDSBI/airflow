from google.cloud import bigquery
from datetime import datetime, timezone, timedelta
import time
import pytz



def etl_pre_payment_deduct_user(target_date: list, client):

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
        MERGE `datahub-478802.datahub.pre_payment_deduct_user` AS target
        USING
        (
        SELECT JoypleGameID
                , AuthMethodID
                , AuthAccountName
                , RegistredTime AS UpdatedTimestamp
        FROM `dataplatform-services.ServiceData.PaymentDeductUser`
        WHERE RegistredTime >= '{start_utc}'
        AND RegistredTime < '{end_utc}'
        AND JoypleGameID IS NOT NULL
        AND AuthMethodID IS NOT NULL
        AND AuthAccountName IS NOT NULL
        ) AS source 
            ON target.joyple_game_code = source.JoypleGameID 
            AND target.auth_method_id = source.AuthMethodID 
            AND target.auth_account_name = source.AuthAccountName
        WHEN MATCHED AND target.update_timestamp <> source.UpdatedTimestamp THEN
        UPDATE SET target.update_timestamp = source.UpdatedTimestamp
        WHEN NOT MATCHED BY target THEN
        INSERT(joyple_game_code, auth_method_id, auth_account_name, update_timestamp)
        VALUES(
                source.JoypleGameID
            , source.AuthMethodID
            , source.AuthAccountName
            , source.UpdatedTimestamp
        )
        WHEN NOT MATCHED BY source THEN
        DELETE 
        """

        # 1. 쿼리 실행
        query_job = client.query(query)

        try:
            # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
            # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
            query_job.result()

            # 3. 성공 시 출력
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} pre_payment_deduct_user Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
            raise e
    
    print("✅ pre_payment_deduct_user ETL 완료")
    return True



def etl_pre_payment_deduct_order(target_date: list, client):

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
        MERGE `datahub-478802.datahub.pre_payment_deduct_order` AS target
        USING
        (
        SELECT JoypleGameID
                , OrderID
                , ProductCode
                , LogTime       AS LogTimestamp
                , RegistredTime AS UpdatedTimestamp
        FROM `dataplatform-services.ServiceData.PaymentDeductOrder`
            WHERE RegistredTime >= '{start_utc}'
            AND RegistredTime < '{end_utc}'
            AND JoypleGameID IS NOT NULL
            AND OrderID IS NOT NULL
            AND ProductCode IS NOT NULL
        ) AS source ON target.joyple_game_code = source.JoypleGameID AND target.order_id = source.OrderID
        WHEN MATCHED AND target.product_code <> source.ProductCode OR target.update_timestamp <> source.UpdatedTimestamp THEN
        UPDATE SET target.product_code = source.ProductCode
                , target.update_timestamp = source.UpdatedTimestamp
        WHEN NOT MATCHED BY target THEN
        INSERT(joyple_game_code, order_id, product_code, update_timestamp)
        VALUES(
            source.JoypleGameID
            , source.OrderID
            , source.ProductCode
            , source.UpdatedTimestamp
        )
        WHEN NOT MATCHED BY source 
            AND (
                target.update_timestamp >= '{start_utc}'
                AND target.update_timestamp < '{end_utc}'
                ) THEN
        DELETE  
        ;
        """

        # 1. 쿼리 실행
        query_job = client.query(query)

        try:
            # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
            # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
            query_job.result()

            # 3. 성공 시 출력
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} pre_payment_deduct_order Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
            raise e
    
    print("✅ pre_payment_deduct_order ETL 완료")
    return True


def etl_pre_payment_info_fix(client):
    truncate_query = f"""
    TRUNCATE TABLE `datahub-478802.datahub.pre_payment_info_fix`
    """

    query = f"""
    INSERT INTO `datahub-478802.datahub.pre_payment_info_fix`
    (
    log_type, 
    joyple_game_code, 
    order_id, 
    market_id, 
    pg_id, 
    platform_device_type, 
    product_code, 
    product_name, 
    currency_code, 
    price, 
    log_timestamp, 
    update_timestamp
    )
    SELECT 
    LogType, 
    JoypleGameID, 
    OrderID, 
    MarketID, 
    PGID, 
    PlatformDeviceType, 
    ProductCode, 
    ProductName,
    CurrencyCode, 
    Price, 
    LogTimestamp, 
    UpdatedTimestamp
    FROM dataplatform-reporting.DataService.T_0141_0000_PaymentInfoFix_V -- DE팀에서 공통로그 테이블을 수정관리해주지 않으면 해당 테이블 관리필요.
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
    
    print("✅ pre_payment_info_fix ETL 완료")


def etl_f_common_payment(target_date: list, client):

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
            -- TA는 V_0150_0000_Payment_V 를 처리한 내용
            -- TB는 V_0153_0000_PaymentDeduct_V 랑 V_0155_0000_PaymentFix_V를 합쳐서 처리한 내용
            -- 이후 insert 처리를 진행해야 함
            
            MERGE datahub-478802.datahub.f_common_payment AS target 
            USING (
            with TA AS (
            SELECT a.GameID
                , a.JoypleGameID
                , a.GameAccountName
                , a.GameSubUserName
                , a.AuthMethodID
                , a.AuthAccountName
                , a.ServerName
                , a.DeviceID
                , a.MarketID
                , a.OSID
                , a.PGID
                , a.GameSubUserLevel
                , a.PlatformDeviceType
                , a.IP                                                                  
                , a.OrderID
                , a.ProductCode
                , a.ProductName
                , a.CurrencyCode
                , a.Price
                , a.LogTime                                            
                , IFNULL(a.MultiQuantity, 1) AS MultiQuantity
            FROM `dataplatform-reporting.DataService.V_0156_0000_CommonLogPaymentFix_V` AS a
            WHERE a.LogTime >= '{start_utc}'
                AND a.LogTime < '{end_utc}'
            )
            , TC as (
            SELECT DATE(a.LogTime, "Asia/Seoul") as datekey
            , d.reg_datekey
            , DATE_DIFF(DATE(a.LogTime, "Asia/Seoul"), d.reg_datekey, DAY) as reg_datediff
            , d.reg_country_code
            , a.GameID as game_id
            , a.JoypleGameID as joyple_game_code
            , a.GameAccountName as game_account_name
            , a.GameSubUserName as game_sub_user_name
            , a.AuthMethodID as auth_method_id
            , a.AuthAccountName as auth_account_name
            , a.ServerName as server_name
            , a.DeviceID as device_id
            , a.IP as ip
            , a.MarketID as market_id
            , a.OSID as os_id
            , a.PGID as pg_id
            , a.PlatformDeviceType as platform_device_type
            , a.GameSubUserLevel as game_user_level
            , a.ProductCode as product_code
            , a.ProductName as product_name
            , a.Price * IFNULL(c.exchange_rate, 1) AS Price_KRW
            , IFNULL(a.MultiQuantity, 1) AS MultiQuantity
            FROM TA as a
            LEFT JOIN (
                SELECT DATE('{start_utc}', "Asia/Seoul") AS datekey
                    , currency
                    , Info.exchange_rate as exchange_rate
                FROM (
                  SELECT currency, ARRAY_AGG(STRUCT(datekey, exchange_rate) ORDER BY datekey DESC LIMIT 1)[OFFSET(0)] AS Info
                  FROM `datahub-478802.datahub.dim_exchange`
                  WHERE datekey >= DATE('{start_utc}')
                    AND datekey <=  DATE('{end_utc}')
                  GROUP BY currency
                ) AS TT
            ) AS c  
            ON a.CurrencyCode = c.currency AND Date(a.LogTime, "Asia/Seoul")  = c.datekey
            LEFT JOIN `datahub-478802.datahub.f_common_register` as d
            on CAST(a.JoypleGameID AS STRING) = CAST(d.joyple_game_code AS STRING) 
               AND CAST(a.AuthAccountName AS STRING) = CAST(d.auth_account_name AS STRING)
            )

                SELECT a.datekey, 
                a.reg_datekey, 
                a.reg_datediff,
                a.reg_country_code,
                a.game_id,
                a.joyple_game_code,
                a.game_account_name,
                a.game_sub_user_name,
                a.auth_method_id,
                a.auth_account_name,
                a.server_name,
                a.device_id,
                b.country_code,
                a.market_id,
                a.os_id,
                a.pg_id,
                a.platform_device_type,
                a.game_user_level,
                product_code,
                product_name,
                sum(price_KRW) as revenue,
                sum(MultiQuantity) as buy_cnt
                FROM TC as a
                LEFT OUTER JOIN datahub-478802.datahub.dim_ip4_country_code AS b
                ON (a.ip = b.ip)
                group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20

                union all

                SELECT a.datekey, 
                       a.reg_datekey, 
                       a.reg_datediff,
                       a.reg_country_code,
                       a.game_id,
                       a.joyple_game_code,
                       a.game_account_name,
                       a.game_sub_user_name,
                       a.auth_method_id,
                       a.auth_account_name,
                       a.server_name,
                       a.device_id,
                       a.country_code,
                       a.market_id,
                       a.os_id,
                       a.pg_id,
                       a.platform_device_type,
                       a.game_user_level,
                       product_code,
                       product_name,
                       sum(price_KRW) as revenue,
                       sum(MultiQuantity) as buy_cnt
                from `datahub-478802.datahub.pre_paymentfix_receipt_after_y24_view` as a 
                WHERE 
                a.log_time >= '{start_utc}'
                AND log_time < '{end_utc}'  
                AND 
                joyple_game_code in (119,123,127,129)
                group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20

            ) as source
            ON target.datekey = source.datekey
            AND target.reg_datekey = source.reg_datekey
            AND target.reg_datediff = source.reg_datediff
            AND target.reg_country_code = source.reg_country_code
            AND target.game_id = source.game_id
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
            AND target.pg_id = source.pg_id
            AND target.platform_device_type = source.platform_device_type
            AND target.game_user_level = source.game_user_level
            AND target.product_code = source.product_code
            AND target.product_name = source.product_name
            WHEN NOT MATCHED BY target THEN 
            INSERT (
            datekey,
            reg_datekey,
            reg_datediff,
            reg_country_code,
            game_id,
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
            pg_id,
            platform_device_type,
            game_user_level,
            product_code,
            product_name,
            revenue,
            buy_cnt
            )
            VALUES (
            source.datekey,
            source.reg_datekey,
            source.reg_datediff,
            source.reg_country_code,
            source.game_id,
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
            source.pg_id,
            source.platform_device_type,
            source.game_user_level,
            source.product_code,
            source.product_name,
            source.revenue,
            source.buy_cnt
            );
        """
        query_job = client.query(query)

        try:
            # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
            # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
            query_job.result()

            # 3. 성공 시 출력
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} f_common_payment Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
            raise e
    
    print("✅ f_common_payment ETL 완료")
    return True