from google.cloud import bigquery
from datetime import datetime, timezone, timedelta
import pytz


def etl_f_common_goods(target_date: list, client):

    kst = pytz.timezone('Asia/Seoul')

    for td_str in target_date:
        try:
            current_date_obj = datetime.strptime(td_str, "%Y-%m-%d")
        except ValueError:
            print(f"⚠️ 날짜 형식이 잘못되었습니다: {td_str}")
            continue

        start_kst = kst.localize(current_date_obj)
        start_utc = start_kst.astimezone(timezone.utc)
        end_kst   = start_kst + timedelta(days=1)
        end_utc   = end_kst.astimezone(timezone.utc)

        print(f"📝 대상날짜: {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"   ㄴ 종료시간(UTC): {end_utc}")

        query = f"""
        MERGE `datahub-478802.datahub.f_common_goods` AS T
        USING (
            WITH GoodsUseCount AS (
                SELECT joyple_game_code
                    , server_name
                    , app_id
                    , DATE(log_time, 'Asia/Seoul')  AS datekey
                    , auth_account_name
                    , game_sub_user_name
                    , game_user_level
                    , market_id
                    , os_id
                    , platform_device_type
                    , mmp_type AS tracker_type_id
                    , goods_name
                    , order_id
                    , CASE WHEN joyple_game_code IN (159,1590) AND free_or_buy = 'buy' AND goods_name = 'gem' AND action_id LIKE '%_FREE_%'
                           THEN REPLACE(action_id, '_FREE_', '_PAID_')
                           ELSE action_id
                      END AS action_id
                    , CASE WHEN joyple_game_code IN (159,1590) AND free_or_buy = 'buy' AND goods_name = 'gem' AND action_name LIKE '%무가 획득%'
                           THEN REPLACE(action_name, '무가 획득', '유가 획득')
                           ELSE action_name
                      END AS action_name
                    , action_category_name
                    , add_or_spend
                    , SUM(IF(free_or_buy = 'free', change_count, 0)) AS free_goods_amount
                    , SUM(IF(free_or_buy = 'buy',  change_count, 0)) AS paid_goods_amount
                    , SUM(change_count)                               AS total_goods_amount
                FROM `dataplatform-204306.CommonLog.Goods`
                WHERE log_time >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                  AND log_time <  TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                GROUP BY joyple_game_code, server_name, app_id, datekey, auth_account_name, game_account_name, game_sub_user_name
                       , game_user_level, market_id, os_id, platform_device_type, mmp_type, goods_name, order_id, action_id, action_name
                       , action_category_name, add_or_spend
            ),
            GoodsUseCount_Package AS (
                SELECT a.*
                     , b.PackageKind
                     , b.CouponKind
                     , b.DiscountRate
                     , b.DiscountPrice
                FROM GoodsUseCount AS a
                LEFT JOIN (
                    SELECT joyple_game_code
                        , order_id
                        , package_kind  AS PackageKind
                        , coupon_kind   AS CouponKind
                        , discount_rate AS DiscountRate
                        , discount_price AS DiscountPrice
                        , ROW_NUMBER() OVER (PARTITION BY joyple_game_code, order_id ORDER BY log_time DESC) AS row_
                    FROM `dataplatform-204306.CommonLog.Package`
                    WHERE log_time >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                      AND log_time <  TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                ) AS b ON a.order_id = b.order_id AND a.joyple_game_code = b.joyple_game_code AND b.row_ = 1
            )

            SELECT joyple_game_code
                 , server_name
                 , app_id
                 , datekey
                 , auth_account_name
                 , game_sub_user_name
                 , game_user_level
                 , market_id
                 , os_id
                 , platform_device_type
                 , tracker_type_id
                 , goods_name
                 , order_id
                 , action_id
                 , action_name
                 , action_category_name
                 , add_or_spend
                 , free_goods_amount
                 , paid_goods_amount
                 , total_goods_amount
            FROM GoodsUseCount_Package
        ) AS S
        ON  T.joyple_game_code       = S.joyple_game_code
        AND T.server_name            = S.server_name
        AND T.app_id                 = S.app_id
        AND T.datekey                = S.datekey
        AND T.auth_account_name      = S.auth_account_name
        AND T.game_sub_user_name     = S.game_sub_user_name
        AND T.game_user_level        = S.game_user_level
        AND T.market_id              = S.market_id
        AND T.os_id                  = S.os_id
        AND T.platform_device_type   = S.platform_device_type
        AND T.tracker_type_id        = S.tracker_type_id
        AND T.goods_name             = S.goods_name
        AND T.order_id               = S.order_id
        AND T.action_id              = S.action_id
        AND T.action_name            = S.action_name
        AND T.action_category_name   = S.action_category_name
        AND T.add_or_spend           = S.add_or_spend
        WHEN MATCHED THEN
            UPDATE SET
                T.free_goods_amount  = S.free_goods_amount,
                T.paid_goods_amount  = S.paid_goods_amount,
                T.total_goods_amount = S.total_goods_amount
        WHEN NOT MATCHED THEN
            INSERT (joyple_game_code, server_name, app_id, datekey, auth_account_name, game_sub_user_name,
                    game_user_level, market_id, os_id, platform_device_type, tracker_type_id, goods_name,
                    order_id, action_id, action_name, action_category_name, add_or_spend,
                    free_goods_amount, paid_goods_amount, total_goods_amount)
            VALUES (S.joyple_game_code, S.server_name, S.app_id, S.datekey, S.auth_account_name, S.game_sub_user_name,
                    S.game_user_level, S.market_id, S.os_id, S.platform_device_type, S.tracker_type_id, S.goods_name,
                    S.order_id, S.action_id, S.action_name, S.action_category_name, S.add_or_spend,
                    S.free_goods_amount, S.paid_goods_amount, S.total_goods_amount)
        """

        query_job = client.query(query)

        try:
            query_job.result()
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} f_common_goods Batch 완료")

        except Exception as e:
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            raise e

    print("✅ f_common_goods ETL 완료")

    return True
