from datetime import datetime, timedelta, timezone
try:
    import gspread
except ImportError:
    print("⚠️  gspread 모듈이 설치되지 않았습니다. 설치 중...")
    import subprocess
    subprocess.check_call(['pip', 'install', 'gspread'])
    import gspread
import io
import os
from google.oauth2.service_account import Credentials
from google.cloud import bigquery, storage
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.decorators import task
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import re
import json
import time
from googleapiclient.discovery import build

# ============= 환경 변수 또는 Airflow Variable 조회 함수 =============
def get_var(key: str, default: str = None) -> str:
    """환경 변수 → Airflow Variable 순서로 조회
    
    Args:
        key: 변수 이름
        default: 기본값 (없으면 None)
    
    Returns:
        환경 변수 값 또는 Airflow Variable 값
    """
    # 1단계: 환경 변수 확인
    env_value = os.environ.get(key)
    if env_value:
        print(f"✓ 환경 변수에서 {key} 로드됨")
        return env_value
    
    # 2단계: Airflow Variable 확인
    try:
        var_value = Variable.get(key, default_var=None)
        if var_value:
            print(f"✓ Airflow Variable에서 {key} 로드됨")
            return var_value
    except Exception as e:
        print(f"⚠️  Variable.get({key}) 오류: {str(e)}")
    
    # 3단계: 기본값 반환
    if default is not None:
        print(f"ℹ️  기본값으로 {key} 설정됨")
        return default
    
    raise ValueError(f"필수 설정 {key}을(를) 찾을 수 없습니다. "
                     f"환경 변수 또는 Airflow Variable에서 설정하세요.")

# 이메일
RECIPIENT_EMAIL = '65e43b85.joycity.com@kr.teams.ms'
SMTP_HOST = get_var("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(get_var("SMTP_PORT", "587"))
SMTP_USER = get_var("SMTP_USER", RECIPIENT_EMAIL)
SMTP_PASSWORD = get_var("SMTP_PASSWORD", "ciucjokomvmipxej")
EMAIL_FROM = get_var("EMAIL_FROM", SMTP_USER)


# ============= 기타 상수 =============
BQ_PROJECT_ID = 'datacatalog-446301'
GCS_BUCKET_NAME = 'uadata_performance_team'

scopes = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]


# ============= Airflow DAG 설정 =============
default_args = {
    'owner': 'data-team',
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
    'start_date': datetime(2024, 1, 1),  # 수정: 고정된 날짜 사용
    'email_on_failure': True,  # 실패 시 자동 이메일
    'email_on_retry': False,
    'email': [RECIPIENT_EMAIL],  # 수신자
}

dag = DAG(
    dag_id='ua_data_for_agency',
    default_args=default_args,
    description='UA Service 일일 배치 작업 (전체 프로젝트, 대행사 보고서, 완료 알림)',
    schedule='0 23 * * *',  # 매일 아침 9시 (UTC 기준)
    catchup=False,
    tags=['ua', 'daily-batch'],
)


# ============= Google Cloud Credentials 설정 =============
print("=" * 80)
print("Google Cloud Credentials 로드 중...")
print("=" * 80)

credentials_info = Variable.get("GOOGLE_CREDENTIAL_JSON", deserialize_json=True)
print(f"✓ Airflow Variable (JSON 문자열)에서 credentials 로드됨")
print(f"✓ Credentials 파싱 성공 (Project: {credentials_info.get('project_id', 'N/A')})")

# 수정: Private key 형식 변환 (\\n → \n)
if 'private_key' in credentials_info:
    credentials_info['private_key'] = credentials_info['private_key'].replace('\\n', '\n')
    print("✓ Private key 형식 수정 완료")
print()

# ============= Task 0: Bigquery 내 UA 데이터 생성 =============
def generate_ua_data_in_bigquery(**context):
    print("=" * 80)
    print("Task 0: Bigquery 내 UA 데이터 생성 시작")
    print("=" * 80)

    bq_client = bigquery.Client.from_service_account_info(credentials_info)

    truncate_query="""
            TRUNCATE TABLE `datacatalog-446301.UA_Service.ru_mailing_data`
            """

    query = """
             
            INSERT INTO `datacatalog-446301.UA_Service.ru_mailing_data`
            (
            project_name, joyple_game_code, regdate_joyple_kst, app_id, gcat, media_category, media, media_source,
            media_detail, product_category, etc_category, optim, campaign, geo, geo_cam, market, Os, os_cam,
            fb_adset_name, fb_adgroup_name, af_siteid, agency, device, setting_title, landing_title, ad_unit,
            mediation, install, RU, rev_D0, rev_D1, rev_D3, rev_D7, rev_D14, rev_D30, rev_D60, rev_D90,
            rev_D120, rev_D150, rev_D180, rev_D210, rev_D240, rev_D270, rev_D300, rev_D330, rev_D360, rev_D390,
            rev_D420, rev_D450, rev_D480, rev_D510, rev_Dcum, rev_iaa_D0, rev_iaa_D1, rev_iaa_D3, rev_iaa_D7,
            rev_iaa_D14, rev_iaa_D30, rev_iaa_D60, rev_iaa_D90, rev_iaa_D120, rev_iaa_D150, rev_iaa_D180,
            rev_iaa_D210, rev_iaa_D240, rev_iaa_D270, rev_iaa_D300, rev_iaa_D330, rev_iaa_D360, rev_iaa_D390,
            rev_iaa_D420, rev_iaa_D450, rev_iaa_D480, rev_iaa_D510, rev_iaa_Dcum, RU_D1, RU_D3, RU_D7, RU_D14,
            RU_D30, RU_D60, RU_D90, Pu_D0, Pu_D1, Pu_D3, Pu_D7, Pu_D14, Pu_D30, Pu_D60, Pu_D90, Pu_D120, Pu_D150, Pu_D180, Pu_D210
            , Pu_D240, Pu_D270, Pu_D300, Pu_D330, Pu_D360, Pu_D390, Pu_D420, Pu_D450, Pu_D480, Pu_D510
            )

            with UAPerfoRowStep1
            AS
            (
              SELECT a.JoypleGameID                                          AS JoypleGameID
                    , a.AuthMethodID                                          AS AuthMethodID
                    , a.AuthAccountName                                       AS AuthAccountName
                    , DATE_DIFF(a.AccessDateKST,a.RegDateKST, DAY)            AS DayDiff
                    , ROW_NUMBER() OVER (PARTITION BY a.JoypleGameID, a.AuthMethodID, a.AuthAccountName ORDER BY a.AccessDateKST ASC) AS Idx
                    , MAX(a.PriceKRW)                                         AS PriceKRW
              FROM (
                SELECT  a.JoypleGameID
                      , a.AuthMethodID
                      , a.AuthAccountName
                      , a.LogDateKST          AS AccessDateKST
                      , AuthAccountRegDateKST AS RegDateKST
                      , PriceKRW
                FROM `dataplatform-reporting.DataService.V_0317_0000_AuthAccountPerformance_V` AS a
                WHERE AuthAccountRegDateKST >= '2017-04-27'
                AND DATE_DIFF(a.LogDateKST,a.AuthAccountRegDateKST, DAY) >= 0
              ) AS a
              GROUP BY a.JoypleGameID, a.AuthMethodID, a.AuthAccountName, a.AccessDateKST, a.RegDateKST  
            ),
            UAPerfoRowStep2
            AS
            (
              SELECT JoypleGameID, AuthMethodID, AuthAccountName
                  , MAX(IF(DayDiff  =                0  and pricecum > 0,  1, 0)) AS d0pu
                  , MAX(IF(DayDiff  between 0 and    1  and pricecum > 0,  1, 0)) AS d1pu
                  , MAX(IF(DayDiff  between 0 and    3  and pricecum > 0,  1, 0)) AS d3pu
                  , MAX(IF(DayDiff  between 0 and    7  and pricecum > 0,  1, 0)) AS d7pu
                  , MAX(IF(DayDiff  between 0 and   14  and pricecum > 0,  1, 0)) AS d14pu
                  , MAX(IF(DayDiff  between 0 and   30  and pricecum > 0,  1, 0)) AS d30pu
                  , MAX(IF(DayDiff  between 0 and   60  and pricecum > 0,  1, 0)) AS d60pu
                  , MAX(IF(DayDiff  between 0 and   90  and pricecum > 0,  1, 0)) AS d90pu
                  , MAX(IF(DayDiff  between 0 and  120  and pricecum > 0,  1, 0)) AS d120pu
                  , MAX(IF(DayDiff  between 0 and  150  and pricecum > 0,  1, 0)) AS d150pu
                  , MAX(IF(DayDiff  between 0 and  180  and pricecum > 0,  1, 0)) AS d180pu
                  , MAX(IF(DayDiff  between 0 and  210  and pricecum > 0,  1, 0)) AS d210pu
                  , MAX(IF(DayDiff  between 0 and  240  and pricecum > 0,  1, 0)) AS d240pu
                  , MAX(IF(DayDiff  between 0 and  270  and pricecum > 0,  1, 0)) AS d270pu
                  , MAX(IF(DayDiff  between 0 and  300  and pricecum > 0,  1, 0)) AS d300pu
                  , MAX(IF(DayDiff  between 0 and  330  and pricecum > 0,  1, 0)) AS d330pu
                  , MAX(IF(DayDiff  between 0 and  360  and pricecum > 0,  1, 0)) AS d360pu
                  , MAX(IF(DayDiff  between 0 and  390  and pricecum > 0,  1, 0)) AS d390pu
                  , MAX(IF(DayDiff  between 0 and  420  and pricecum > 0,  1, 0)) AS d420pu
                  , MAX(IF(DayDiff  between 0 and  450  and pricecum > 0,  1, 0)) AS d450pu
                  , MAX(IF(DayDiff  between 0 and  480  and pricecum > 0,  1, 0)) AS d480pu
                  , MAX(IF(DayDiff  between 0 and  510  and pricecum > 0,  1, 0)) AS d510pu
              FROM (
             
                SELECT JoypleGameID, AuthMethodID, AuthAccountName, IFNULL(DayDiff, -1) AS DayDiff, MAX(PriceKRW) AS PriceKRW, MAX(PriceCum) AS PriceCum, 1 AS Access
                FROM (
                  SELECT JoypleGameID, AuthMethodID, AuthAccountName
                      , CASE WHEN b.Num = 1 THEN DayDiff ELSE null END AS DayDiff
                      , CASE WHEN b.Num = 1 THEN PriceKRW ELSE 0 END AS PriceKRW
                      , PriceCum
                      , b.Num
                  FROM (
                    SELECT JoypleGameID, AuthMethodID, AuthAccountName, idx, DayDiff, PriceKRW
                        , SUM(PriceKRW) OVER (PARTITION BY JoypleGameID, AuthAccountName, AuthMethodID ORDER BY idx ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as PriceCum  
                    FROM UAPerfoRowStep1
                  ) AS a
                  CROSS JOIN
                  (SELECT 1 AS NUM UNION ALL SELECT 2) AS b
                )
                GROUP BY JoypleGameID, AuthMethodID, AuthAccountName, IFNULL(DayDiff, -1)
              )
              WHERE DayDiff BETWEEN - 1 AND 510
              GROUP BY JoypleGameID, AuthMethodID, AuthAccountName
            )
            , PU_table AS
            (
            SELECT a.JoypleGameID AS JoypleGameID
                , RegdateDateKst AS RegdateAuthAccountDateKST, AppID, MediaSource, Campaign, CountryCode, MarketName, OS, AdsetName, AdName, Agency, SiteID
                , Pu_D0, Pu_D1, Pu_D3, Pu_D7, Pu_D14, Pu_D30, Pu_D60, Pu_D90, Pu_D120, Pu_D150, Pu_D180
                , Pu_D210, Pu_D240, Pu_D270, Pu_D300, Pu_D330, Pu_D360, Pu_D390, Pu_D420, Pu_D450, Pu_D480, Pu_D510
            FROM (
                SELECT a.JoypleGameID
                    , b.AuthAccountRegDateKST AS RegdateDateKst
                    , CASE WHEN b.CountryCode = '' OR b.CountryCode IS NULL THEN 'NULL' ELSE b.CountryCode END AS CountryCode -- 추가
                    , CASE WHEN b.MarketName  = '' OR b.MarketName  IS NULL THEN 'NULL' ELSE b.MarketName  END AS MarketName -- 추가
                    , b.OS
                    , CASE WHEN b.MediaSource = '' OR b.MediaSource IS NULL THEN 'NULL' ELSE b.MediaSource END AS MediaSource
                    , CASE WHEN b.Campaign    = '' OR b.Campaign    IS NULL THEN 'NULL' ELSE b.Campaign    END AS Campaign
                    , CASE WHEN b.AdsetName   = '' OR b.AdsetName   IS NULL THEN 'NULL' ELSE b.AdsetName   END AS AdsetName
                    , CASE WHEN b.AdName      = '' OR b.AdName      IS NULL THEN 'NULL' ELSE b.AdName      END AS AdName
                    , CASE WHEN b.Agency      = '' OR b.Agency      IS NULL THEN 'NULL' ELSE b.Agency      END AS Agency
                    , CASE WHEN b.SiteID      = '' OR b.SiteID      IS NULL THEN 'NULL' ELSE b.SiteID      END AS SiteID
                    , CASE WHEN b.AppID       = '' OR b.AppID       IS NULL THEN 'NULL' ELSE b.AppID       END AS AppID
                    , SUM(d0pu)   AS Pu_D0
                    , SUM(d1pu)   AS Pu_D1
                    , SUM(d3pu)   AS Pu_D3
                    , SUM(d7pu)   AS Pu_D7
                    , SUM(d14pu)  AS Pu_D14
                    , SUM(d30pu)  AS Pu_D30
                    , SUM(d60pu)  AS Pu_D60
                    , SUM(d90pu)  AS Pu_D90
                    , SUM(d120pu) AS Pu_D120
                    , SUM(d150pu) AS Pu_D150
                    , SUM(d180pu) AS Pu_D180
                    , SUM(d210pu) AS Pu_D210
                    , SUM(d240pu) AS Pu_D240
                    , SUM(d270pu) AS Pu_D270
                    , SUM(d300pu) AS Pu_D300
                    , SUM(d330pu) AS Pu_D330
                    , SUM(d360pu) AS Pu_D360    
                    , SUM(d390pu) AS Pu_D390
                    , SUM(d420pu) AS Pu_D420
                    , SUM(d450pu) AS Pu_D450
                    , SUM(d480pu) AS Pu_D480
                    , SUM(d510pu) AS Pu_D510
                FROM UAPerfoRowStep2 AS a
                LEFT OUTER JOIN `dataplatform-reporting.DataService.V_0316_0000_AuthAccountInfo_V` AS b
                ON (a.JoypleGameID = b.JoypleGameID AND a.AuthMethodID = b.AuthMethodID AND a.AuthAccountName = b.AuthAccountName)
                GROUP BY JoypleGameID, RegdateDateKst, CountryCode, MarketName, OS, MediaSource, Campaign, AdsetName, AdName, Agency, SiteID, AppID
            ) AS  a
            )

            , UA_perfo as (
            select c.JoypleGameName as ProJect_name, a.JoypleGameID, a.RegdateAuthAccountDateKST, a.APPID,
                a.MediaSource, a.CamPaign
                , b.UptdtCampaign
                , case when a.MediaSource in ('Unknown', 'NULL') then 'Unknown'                
                        when a.campaign like '%Pirates of the Caribbean Android AU%' then 'UA'  ## 과거 POTC 캠페인 케이스는 여전히 하드 코딩을 해야함.
                        when a.campaign like '%Pirates of the Caribbean Android KR%' then 'UA'  
                        when a.campaign like '%Pirates of the Caribbean Android US%' then 'UA'
                        when a.campaign like '%Pirates of the Caribbean Android GB%' then 'UA'  
                        when a.campaign = 'POTC_検索' then 'UA'
                        when b.gcat is null and c.JoypleGameName ='POTC' then d.gcat ## 이와 같이 처리를 해도 gcat이 Null 값인 것은 있음. => Mas에 등록이안된 Case들 => 근본적인 해결책이 있을까 ..?
                    else b.gcat
                    end as gcat
                , a.CountryCode, a.MarketName, a.OS, a.AdsetName, a.AdName, a.Agency, a.SiteID
                , a.TrackerInstallCount, a.RU
                , a.rev_d0, a.rev_d1, a.rev_d3, a.rev_d7, a.rev_d14, a.rev_d30, a.rev_d60
                , a.rev_d90, a.rev_d120, a.rev_d150, a.rev_d180, a.rev_d210, a.rev_d240
                , a.rev_d270, a.rev_d300, a.rev_d330, a.rev_d360, a.rev_d390, a.rev_d420, a.rev_d450
                , a.rev_d480, a.rev_d510, a.rev_dcum
                , rev_iaa_D0, rev_iaa_D1, rev_iaa_D3, rev_iaa_D7, rev_iaa_D14, rev_iaa_D30, rev_iaa_D60, rev_iaa_D90
                , rev_iaa_D120, rev_iaa_D150, rev_iaa_D180, rev_iaa_D210, rev_iaa_D240, rev_iaa_D270, rev_iaa_D300, rev_iaa_D330
                , rev_iaa_D360, rev_iaa_D390, rev_iaa_D420, rev_iaa_D450, rev_iaa_D480, rev_iaa_D510, rev_iaa_Dcum
                , a.ru_d1, a.ru_d3, a.ru_d7, a.ru_d14, a.ru_d30, a.ru_d60, a.ru_d90
                , a.ru_d120, a.ru_d150, a.ru_d180
                , a.Pu_D0, a.Pu_D1, a.Pu_D3, a.Pu_D7, a.Pu_D14, a.Pu_D30, a.Pu_D60, a.Pu_D90, a.Pu_D120, a.Pu_D150, a.Pu_D180, a.Pu_D210
                , a.Pu_D240, a.Pu_D270, a.Pu_D300, a.Pu_D330, a.Pu_D360, a.Pu_D390, a.Pu_D420, a.Pu_D450, a.Pu_D480, a.Pu_D510
                , case  when a.campaign like '%Pirates of the Caribbean Android AU%' then 'ADNW'
                        when a.campaign like '%Pirates of the Caribbean Android KR%' then 'ADNW'
                        when a.campaign like '%Pirates of the Caribbean Android US%' then 'ADNW'
                        when a.campaign like '%Pirates of the Caribbean Android GB%' then 'ADNW'
                        when a.campaign = 'POTC_検索' then 'ADNW'
                        when b.gcat is null and c.JoypleGameName ='POTC' then d.media_category
                        else b.mediacategory
                    end as mediacategory
                , b.productcategory, b.media, b.mediadetail
                , case when b.optim  = 'NONE' and a.AdsetName like '%MAIA%' then 'MAIA'
                        when b.optim  = 'NONE' and a.AdsetName like '%AEO%' then 'AEO'
                        when b.optim  = 'NONE' and a.AdsetName like '%VO%' then 'VO'
                    else b.optim end as optim
                , b.etccategory,  b.OSCAM, b.GEOCAM, datecam
                , b.creativeno , b.device, b.settingtitle, b.landingtitle, b.adunit, b.mediation
                , b.createyn, b.updateyn, b.ruleyn
            from
            (
                select perfo.*, rev_iaa_D0, rev_iaa_D1, rev_iaa_D3, rev_iaa_D7, rev_iaa_D14, rev_iaa_D30, rev_iaa_D60, rev_iaa_D90
                            , rev_iaa_D120, rev_iaa_D150, rev_iaa_D180, rev_iaa_D210, rev_iaa_D240, rev_iaa_D270, rev_iaa_D300, rev_iaa_D330
                            , rev_iaa_D360, rev_iaa_D390, rev_iaa_D420, rev_iaa_D450, rev_iaa_D480, rev_iaa_D510, rev_iaa_Dcum
                            , Pu_D0, Pu_D1, Pu_D3, Pu_D7, Pu_D14, Pu_D30, Pu_D60, Pu_D90, Pu_D120, Pu_D150, Pu_D180, Pu_D210
                            , Pu_D240, Pu_D270, Pu_D300, Pu_D330, Pu_D360, Pu_D390, Pu_D420, Pu_D450, Pu_D480, Pu_D510
                from `dataplatform-reporting.DataService.T_0420_0000_UAPerformanceRaw_V1` as perfo
                left join `dataplatform-reporting.DataService.T_0422_0000_UAPerformanceInAppADRaw_V1` as inapp
                on  perfo.JoypleGameID = inapp.JoypleGameID
                and perfo.RegdateAuthAccountDateKST = inapp.RegdateAuthAccountDateKST
                and perfo.AppID = inapp.AppID
                and perfo.MediaSource = inapp.MediaSource
                and perfo.Campaign = inapp.Campaign
                and perfo.CountryCode = inapp.CountryCode
                and perfo.MarketName = inapp.MarketName
                and perfo.OS = inapp.OS
                and perfo.AdsetName = inapp.AdsetName
                and perfo.AdName = inapp.AdName
                and perfo.Agency = inapp.Agency
                and perfo.SiteID = inapp.SiteID
                left join PU_table as put
                on  perfo.JoypleGameID = put.JoypleGameID
                and perfo.RegdateAuthAccountDateKST = put.RegdateAuthAccountDateKST
                and perfo.AppID = put.AppID
                and perfo.MediaSource = put.MediaSource
                and perfo.Campaign = put.Campaign
                and perfo.CountryCode = put.CountryCode
                and perfo.MarketName = put.MarketName
                and perfo.OS = put.OS
                and perfo.AdsetName = put.AdsetName
                and perfo.AdName = put.AdName
                and perfo.Agency = put.Agency
                and perfo.SiteID = put.SiteID

            ) as a
            left join (select distinct *
                    from `dataplatform-reporting.DataService.V_0261_0000_AFCampaignRule_V`) as b
            on a.appID = b.appID and a.MediaSource = b.MediaSource and a.Campaign = b.initCampaign
            left join `dataplatform-reporting.DataService.V_0200_0001_GameJoyple_V` as c
            on a.joypleGameid = c.JoypleGameid
            left join `data-science-division-216308.POTC.before_mas_campaign` as d
            on a.campaign = d.campaign
            )
           
            , final as (
            select
            project_name
            , JoypleGameID as joyple_game_code
            , RegdateAuthAccountDateKST as regdate_joyple_kst
            , appid as app_id
            , gcat
            ,CASE WHEN mediasource = 'Organic' then 'Organic'
                when mediasource = 'Unknown' then 'Unknown'
                else mediacategory
            end as media_category
            ,CASE WHEN mediasource = 'Organic' then 'Organic'
                when mediasource = 'Unknown' then 'Unknown'
                else media
            end as media
            , mediasource as media_source
            , mediadetail as media_detail
            , productcategory as product_category
            , etccategory as etc_category
            , optim
            , uptdtcampaign as campaign
            , countrycode as geo
            , geocam  as geo_cam
            , marketname as market
            , OS
            , oscam as os_cam
            , adsetname as fb_adset_name
            , AdName as fb_adgroup_name
            , siteid as af_siteid
            , agency
            , device
            , settingtitle as setting_title
            , landingtitle as landing_title
            , adunit as ad_unit
            , mediation as mediation
            , TrackerInstallCount as install
            , RU
            , rev_D0, rev_D1,rev_D3,rev_D7,rev_D14,rev_D30,rev_D60,rev_D90,rev_D120,rev_D150,rev_D180,rev_D210,rev_D240,rev_D270,rev_D300,rev_D330,rev_D360,rev_D390,rev_D420,rev_D450,rev_D480,rev_D510,rev_Dcum
            , rev_iaa_D0, rev_iaa_D1, rev_iaa_D3, rev_iaa_D7, rev_iaa_D14, rev_iaa_D30, rev_iaa_D60, rev_iaa_D90
            , rev_iaa_D120, rev_iaa_D150, rev_iaa_D180, rev_iaa_D210, rev_iaa_D240, rev_iaa_D270, rev_iaa_D300, rev_iaa_D330
            , rev_iaa_D360, rev_iaa_D390, rev_iaa_D420, rev_iaa_D450, rev_iaa_D480, rev_iaa_D510, rev_iaa_Dcum
            , RU_D1,RU_D3,RU_D7,RU_D14,RU_D30,RU_D60,RU_D90
            , Pu_D0, Pu_D1, Pu_D3, Pu_D7, Pu_D14, Pu_D30, Pu_D60, Pu_D90, Pu_D120, Pu_D150, Pu_D180, Pu_D210
                            , Pu_D240, Pu_D270, Pu_D300, Pu_D330, Pu_D360, Pu_D390, Pu_D420, Pu_D450, Pu_D480, Pu_D510
            from UA_perfo
            where RegdateAuthAccountDateKST >= date_add(current_date('Asia/Seoul'),interval -13 month)
            and RegdateAuthAccountDateKST <= date_add(current_date('Asia/Seoul'),interval -1 day)
            )
            select project_name, joyple_game_code, regdate_joyple_kst, app_id, gcat, media_category, media, media_source,
            media_detail, product_category, etc_category, optim, campaign, geo, geo_cam, market, Os, os_cam,
            fb_adset_name, fb_adgroup_name, af_siteid, agency, device, setting_title, landing_title, ad_unit,
            mediation, install, RU, rev_D0, rev_D1, rev_D3, rev_D7, rev_D14, rev_D30, rev_D60, rev_D90,
            rev_D120, rev_D150, rev_D180, rev_D210, rev_D240, rev_D270, rev_D300, rev_D330, rev_D360, rev_D390,
            rev_D420, rev_D450, rev_D480, rev_D510, rev_Dcum, rev_iaa_D0, rev_iaa_D1, rev_iaa_D3, rev_iaa_D7,
            rev_iaa_D14, rev_iaa_D30, rev_iaa_D60, rev_iaa_D90, rev_iaa_D120, rev_iaa_D150, rev_iaa_D180,
            rev_iaa_D210, rev_iaa_D240, rev_iaa_D270, rev_iaa_D300, rev_iaa_D330, rev_iaa_D360, rev_iaa_D390,
            rev_iaa_D420, rev_iaa_D450, rev_iaa_D480, rev_iaa_D510, rev_iaa_Dcum, RU_D1, RU_D3, RU_D7, RU_D14,
            RU_D30, RU_D60, RU_D90, Pu_D0, Pu_D1, Pu_D3, Pu_D7, Pu_D14, Pu_D30, Pu_D60, Pu_D90, Pu_D120, Pu_D150, Pu_D180, Pu_D210
            , Pu_D240, Pu_D270, Pu_D300, Pu_D330, Pu_D360, Pu_D390, Pu_D420, Pu_D450, Pu_D480, Pu_D510
            from final
            """
    
    bq_client.query(truncate_query)
    time.sleep(5)
    bq_client.query(query)
    print("=" * 80)
    print("✓ Bigquery 내 UA 데이터 생성 완료")
    print("=" * 80)
    


# ============= Task 1: 전체 프로젝트 보고서 생성 및 업로드 =============
def generate_all_projects_reports(**context):
    """전체 프로젝트 보고서 생성 및 GCS 업로드 (요약 결과를 한 번만 XCom에 푸시)"""
    print("=" * 80)
    print("Task 1: 전체 프로젝트 보고서 생성 시작")
    print("=" * 80)
    
    creds = Credentials.from_service_account_info(credentials_info, scopes=scopes)
    client = gspread.authorize(creds)
    bq_client = bigquery.Client.from_service_account_info(credentials_info)
    storage_client = storage.Client.from_service_account_info(credentials_info)
    
    spreadsheet_url = "https://docs.google.com/spreadsheets/d/1gMEd4_sTX-Y1jr4JcZyonDiMzazKfJOjHdahvhBKNGE/edit?gid=0#gid=0"
    spreadsheet = client.open_by_url(spreadsheet_url)
    worksheet = spreadsheet.sheet1
    data = worksheet.get_all_values()
    project_list = [cell for row in data for cell in row][1:]
    
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    today = datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d')    
    two_days_ago = (datetime.now(timezone(timedelta(hours=9))) - timedelta(days=2)).strftime('%Y-%m-%d')

    failed_projects = []
    total_rows = 0
    uploaded_files = []
    
    for project_name in project_list:
        if not project_name.strip():
            continue
        try:
            query = """
                SELECT * except(  Pu_D0, Pu_D1, Pu_D3, Pu_D7, Pu_D14, Pu_D30, Pu_D60, Pu_D90, Pu_D120, Pu_D150, Pu_D180, Pu_D210
                                , Pu_D240, Pu_D270, Pu_D300, Pu_D330, Pu_D360, Pu_D390, Pu_D420, Pu_D450, Pu_D480, Pu_D510)
                FROM `datacatalog-446301.UA_Service.ru_mailing_data`
                WHERE
                    project_name = @project_name
                    AND regdate_joyple_kst BETWEEN DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 3 MONTH) 
                                               AND DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("project_name", "STRING", project_name)]
            )
            print(f"[프로젝트: {project_name}] 쿼리 실행 중...")
            df = bq_client.query(query, job_config=job_config).to_dataframe()
            
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            blob_name = f"all_reports/{project_name}_{today}.csv"
            blob = bucket.blob(blob_name)
            blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv; charset=utf-8-sig")
            print(f"✓ GCS 업로드 완료: {blob.name}")
            
            expiration = timedelta(hours=24)
            signed_url = blob.generate_signed_url(version="v4", expiration=expiration, method="GET")
            cell = worksheet.find(project_name)
            if cell:
                worksheet.update_cell(cell.row, cell.col + 1, signed_url)
                current_time = datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M:%S')
                worksheet.update_cell(cell.row, cell.col + 2, current_time)
            uploaded_files.append({'project': project_name, 'file': blob.name, 'url': signed_url, 'rows': len(df)})
            total_rows += len(df)
            
            old_blob_name = f"all_reports/{project_name}_{two_days_ago}.csv"
            old_blob = bucket.blob(old_blob_name)
            if old_blob.exists():
                old_blob.delete()
        except Exception as e:
            print(f"✗ [{project_name}] 오류 발생: {str(e)}")
            failed_projects.append({'project': project_name, 'error': str(e)})
            continue
    
    # 요약 결과를 한 번만 XCom에 푸시
    summary = {
        'status': 'success' if not failed_projects else 'partial_failed',
        'total_projects': len(project_list),
        'total_rows': total_rows,
        'uploaded_files': uploaded_files,
        'failed_projects': failed_projects
    }
    context['ti'].xcom_push(key='all_projects_report', value=summary)
    
    if failed_projects:
        print("\n⚠️  실패한 프로젝트 목록:")
        for f in failed_projects:
            print(f"  - {f['project']}: {f['error']}")
    print("=" * 80)
    print("Task 1: 전체 프로젝트 보고서 생성 완료")
    print("=" * 80)


# ============= Task 2: 대행사 보고서 생성 및 업로드 =============
def generate_agency_reports(**context):
    """대행사 보고서 생성 및 GCS 업로드"""
    print("=" * 90)
    print("Task 2: 대행사 보고서 생성 시작")
    print("=" * 90)
    
    # 수정: info → credentials_info
    # Google Sheets 클라이언트 초기화
    creds = Credentials.from_service_account_info(credentials_info, scopes=scopes)
    client = gspread.authorize(creds)
    
    # BigQuery 및 Storage 클라이언트 초기화
    bq_client = bigquery.Client.from_service_account_info(credentials_info)
    storage_client = storage.Client.from_service_account_info(credentials_info)
    
    # 스프레드시트에서 프로젝트 및 대행사 목록 가져오기
    spreadsheet_url = "https://docs.google.com/spreadsheets/d/1gMEd4_sTX-Y1jr4JcZyonDiMzazKfJOjHdahvhBKNGE/edit?gid=1879882444#gid=1879882444"
    spreadsheet = client.open_by_url(spreadsheet_url)
    worksheet = spreadsheet.worksheet('sheet2')
    
    project_list = worksheet.col_values(1)[1:]  # A열
    agency_list = worksheet.col_values(2)[1:]   # B열
    
    print(f"프로젝트 리스트: {project_list}")
    print(f"대행사 리스트: {agency_list}")
    
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    today = datetime.today().strftime('%Y-%m-%d')
    two_days_ago = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')  # 수정: 날짜 형식 통일
    
    columns_to_select = """
        project_name, joyple_game_code, regdate_joyple_kst, app_id, gcat,
        media_category, media, product_category, media_detail, optim,
        etc_category, device, setting_title, landing_title, ad_unit,
        mediation, media_source, campaign, geo, geo_cam, market, Os,
        os_cam, fb_adset_name, fb_adgroup_name, af_siteid, agency,
        install, RU, rev_D0, rev_D1, rev_D3, rev_D7, rev_D14, rev_D30,
        rev_D60, rev_D90, rev_D120, rev_D150, rev_D180, rev_D210,
        rev_D240, rev_D270, rev_D300, rev_D330, rev_D360, rev_D390,
        rev_D420, rev_D450, rev_D480, rev_D510, rev_Dcum, RU_D1,
        RU_D3, RU_D7, RU_D14, RU_D30, RU_D60, RU_D90, 
        PU_D0,   PU_D1,   PU_D3,   PU_D7,   PU_D14,  PU_D30,
        PU_D60,  PU_D90,  PU_D120, PU_D150, PU_D180, PU_D210,
        PU_D240, PU_D270, PU_D300, PU_D330, PU_D360, PU_D390,
        PU_D420, PU_D450, PU_D480, PU_D510
    """
    
    # 실패한 작업 추적
    failed_reports = []
    success_count = 0
    
    for i in range(len(project_list)):
        pr_name = project_list[i].strip() if i < len(project_list) else ""
        agency_name = agency_list[i].strip() if i < len(agency_list) else ""
        
        if not pr_name or not agency_name:
            continue
        
        try:
            # 대행사별 필터 조건 설정
            agency_filter_condition = ""
            if agency_name == 'dotlab':
                agency_filter_condition = "AND media_category in ('ADNW', 'adnw-pre', 'ADNW-Pre') AND agency IN ('dotlab','NULL', 'webmediail626', 'tyrads')"
            elif agency_name == 'Nasmedia':
                agency_filter_condition = """
                AND (
                    (media_category = 'ADNW' AND agency = 'Nasmedia') OR
                    (media_category = 'ADNW' AND agency = 'dotlab' AND regdate_joyple_kst >= '2023-04-01') OR
                    (media_category = 'ADNW' AND agency = 'NULL' AND regdate_joyple_kst >= '2023-03-01' AND media_source NOT IN ('Ironsource', 'Tapjoy', 'Myappfree') AND campaign != 'ES_And_Mistplay_nCPI' ) OR
                    (media_category = 'ADNW' AND agency = 'NULL' AND regdate_joyple_kst >= '2023-03-08') OR
                    (campaign = 'US_And_Myappfree_CPA_MRP') OR
                    (media_category = 'ADNW' AND agency = 'NULL' AND regdate_joyple_kst >= '2023-03-13' AND media_source IN ('Ironsource')) OR
                    (media_category = 'ADNW' AND agency = 'NULL' AND regdate_joyple_kst >= '2023-03-20' AND media_source IN ('Tapjoy'))
                )
                """
            
            # 수정: SQL Injection 방지를 위한 파라미터화된 쿼리
            base_query = f"""
                SELECT {columns_to_select}
                FROM `datacatalog-446301.UA_Service.ru_mailing_data`
                WHERE project_name = @project_name
                  AND regdate_joyple_kst >= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 3 MONTH)
                {agency_filter_condition}
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("project_name", "STRING", pr_name)
                ]
            )
            
            print(f"[프로젝트: {pr_name}, 대행사: {agency_name}] 쿼리 실행 중...")
            df = bq_client.query(base_query, job_config=job_config).to_dataframe()
            
            # CSV로 변환
            # csv_buffer = io.StringIO()
            # df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            
            # # GCS에 업로드
            # blob_name = f"agency_reports/{pr_name}_{agency_name}_{today}.csv"
            # blob = bucket.blob(blob_name)
            # blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv; charset=utf-8-sig")

            # print(f"✓ GCS 업로드 완료: {blob.name}")
            
            # 1. StringIO 대신 BytesIO 사용 (바이트 단위 처리)
            csv_buffer = io.BytesIO()
            
            # 2. encoding='utf-8-sig' 적용 (Pandas가 바이트로 변환할 때 BOM을 삽입함)
            df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            
            # 3. GCS에 업로드 (이미 바이트 상태이므로 추가 인코딩 없이 그대로 올라감)
            blob_name = f"agency_reports/{pr_name}_{agency_name}_{today}.csv"
            blob = bucket.blob(blob_name)
            
            # 중요: content_type에 charset은 빼고 전송 (파일 자체가 이미 -sig이므로)
            blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")
            
            print(f"✓ GCS 업로드 완료 (BOM 적용): {blob.name}")

            # 수정: Signed URL 생성 (v4 방식 명시)
            expiration = timedelta(hours=24)
            signed_url = blob.generate_signed_url(
                version="v4",
                expiration=expiration,
                method="GET"
            )
            
            # 스프레드시트 업데이트
            all_cells = worksheet.findall(pr_name)
            found = False
            
            for cell in all_cells:
                row_values = worksheet.row_values(cell.row)
                if pr_name in row_values and agency_name in row_values:
                    agency_col = row_values.index(agency_name) + 1
                    worksheet.update_cell(cell.row, agency_col + 2, signed_url)
                    current_time = datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M:%S')
                    worksheet.update_cell(cell.row, agency_col + 4, current_time)
                    print(f"✓ [{pr_name}, {agency_name}] 업로드 완료 및 URL 업데이트")
                    found = True
                    break
            
            if not found:
                worksheet.append_row([pr_name, agency_name, signed_url])
                print(f"✓ [{pr_name}, {agency_name}] 새로운 행으로 추가")
            
            success_count += 1

            # 2일 전 파일 삭제
            old_blob_name = f"agency_reports/{pr_name}_{agency_name}_{two_days_ago}.csv"
            old_blob = bucket.blob(old_blob_name)
            if old_blob.exists():
                old_blob.delete()
                print(f"  → 이전 파일 삭제: {old_blob_name}")
                
        except Exception as e:
            # 수정: 개별 보고서 실패 시 전체 중단하지 않고 계속 진행
            print(f"✗ [{pr_name}, {agency_name}] 오류 발생: {str(e)}")
            failed_reports.append((pr_name, agency_name, str(e)))
            continue
    
    # XCom에 대행사 보고서 결과 푸시
    summary = {
        'status': 'success' if not failed_reports else 'partial_failed',
        'success_count': success_count,
        'total_reports': len(project_list),
        'failed_reports': failed_reports
    }
    context['ti'].xcom_push(key='agency_reports', value=summary)

    # 실패한 보고서가 있으면 경고 출력
    if failed_reports:
        print("\n⚠️  실패한 보고서 목록:")
        for report in failed_reports:
            print(f"  - {report['project']} / {report['agency']}: {report['error']}")
    
    print("=" * 80)
    print("Task 2: 대행사 보고서 생성 완료")

# ============= Task 3: 권한 부여 진행하기 =============
def authorize_gcs_access(**context):
    creds = Credentials.from_service_account_info(credentials_info, scopes=scopes)
    client = gspread.authorize(creds)

    spreadsheet_url = "https://docs.google.com/spreadsheets/d/1gMEd4_sTX-Y1jr4JcZyonDiMzazKfJOjHdahvhBKNGE/edit?gid=1879882444#gid=1879882444"
    spreadsheet = client.open_by_url(spreadsheet_url)
    worksheet = spreadsheet.worksheet('sheet2')

    mail_list = worksheet.col_values(3)[1:]
    targeturl_list = worksheet.col_values(5)[1:]
    link_list = worksheet.col_values(4)[1:]

    # Google Drive 클라이언트
    drive_service = build('drive', 'v3', credentials=creds)
    sheets_service = build('sheets', 'v4', credentials=creds)

# 각 행마다 처리
    for idx, (mails_str, target_url_str, link) in enumerate(zip(mail_list, targeturl_list, link_list)):
        if not mails_str or not target_url_str or not link:  # 빈 셀 건너뛰기
            continue
        
        print(f"\n--- {idx + 1}번째 행 처리 ---")
        
        # 콤마로 구분된 이메일 파싱 (공백 제거)
        emails = [email.strip() for email in mails_str.split(',') if email.strip()]
        
        print(f"이메일: {emails}")
        print(f"타겟 URL: {target_url_str}")
        print(f"링크: {link}")
        
        # 타겟 URL에서 Spreadsheet ID 추출
        match = re.search(r'/d/([a-zA-Z0-9-_]+)', target_url_str)
        if not match:
            print(f"✗ URL에서 Spreadsheet ID를 추출할 수 없습니다: {target_url_str}")
            continue
        
        target_file_id = match.group(1)
        print(f"타겟 파일 ID: {target_file_id}")
        
        # 각 이메일에 대해 권한 부여
        for email in emails:
            try:
                permission = {
                    'type': 'user',
                    'role': 'reader',
                    'emailAddress': email
                }
                drive_service.permissions().create(
                    fileId=target_file_id,
                    body=permission,
                    fields='id'
                ).execute()
                print(f"  ✓ {email}에게 권한 부여 완료")
            except Exception as e:
                print(f"  ✗ {email} 권한 부여 실패: {str(e)}")
        
        # target_url_str (문자열)을 사용
        try:
            target_sheet = client.open_by_url(target_url_str)
        except Exception as e:
            print(f"✗ 타겟 시트 열기 실패: {str(e)}")
            continue
        
        # target_sheet가 이미 Spreadsheet 객체이므로 .id 직접 사용
        spreadsheet_id = target_sheet.id
        
        # worksheets()를 사용해 첫 번째 시트 가져오기
        try:
            worksheets = target_sheet.worksheets()
            if not worksheets:
                print(f"✗ 시트가 없습니다.")
                continue
            target_sheet_title = worksheets[0].title  # 첫 번째 시트 이름 가져오기
        except Exception as e:
            print(f"✗ 시트 정보 조회 실패: {str(e)}")
            continue
        
        # 현재 행에 해당하는 링크만 업데이트
        update_data = [[link]]  # 단일 셀만 업데이트
        
        # C 열의 행 번호 계산 (idx는 0부터 시작, 헤더는 제외되었으므로 idx+2부터 시작)
        current_time = datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M:%S')
        
        request_body = {
            'data': [
                {
                    'range': f"{target_sheet_title}!C2",  # 현재 행의 C 열만 업데이트
                    'majorDimension': 'ROWS',
                    'values': update_data
                },
                {
                    'range': f"{target_sheet_title}!D2",  # D 열 업데이트
                    'majorDimension': 'ROWS',
                    'values': [[current_time]]
                }
            ],
            'valueInputOption': 'RAW'
        }
        
        try:
            response = sheets_service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=request_body
            ).execute()
            
            print(f"✓ '{target_sheet_title}' 시트의 C{row_number} 셀에 데이터 업데이트 완료!")
            print(f"  업데이트 내용: {link}")
        except Exception as e:
            print(f"✗ 데이터 업데이트 실패: {str(e)}")


# ============= Task 4: 이메일 발송 =============
def send_status_email(**context):
    """작업 완료 후 간단한 알림 이메일 발송"""
    print("=" * 80)
    print("Task 3: 완료 알림 이메일 발송 시작")
    print("=" * 80)
    
    # 실행 날짜 (logical_date 사용)
    logical_date = context['logical_date']
    execution_date = logical_date.strftime('%Y-%m-%d %H:%M:%S KST')
    
    # 이메일 제목 및 본문 작성
    subject = "✅ UA 데이터 배치 작업 완료"
    body = f"""
    UA 데이터 배치 작업이 완료되었습니다.

    실행 시간: {execution_date}

    이 이메일은 Airflow 자동 배치 작업에서 발송되었습니다.
    """
    
    # 이메일 발송
    try:
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = EMAIL_FROM
        msg['To'] = RECIPIENT_EMAIL
        
        text_part = MIMEText(body, 'plain')
        msg.attach(text_part)
        
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
        
        print(f"✓ 이메일 발송 완료: {RECIPIENT_EMAIL}")
        print(f"  제목: {subject}")
    except Exception as e:
        print(f"✗ 이메일 발송 실패: {str(e)}")
        raise
    
    print("=" * 80)
    print("Task 3: 완료 알림 이메일 발송 완료")
    print("=" * 80)


# ============= Airflow Tasks 정의 =============

task_bigquery_projects = PythonOperator(
    task_id='generate_ua_data_in_bigquery',
    python_callable=generate_ua_data_in_bigquery,
    dag=dag,
)

task_all_projects = PythonOperator(
    task_id='generate_all_projects_reports',
    python_callable=generate_all_projects_reports,
    dag=dag,
)

task_agency_reports = PythonOperator(
    task_id='generate_agency_reports',
    python_callable=generate_agency_reports,
    dag=dag,
)

task_authorize_gcs = PythonOperator(
    task_id='authorize_gcs_access',
    python_callable=authorize_gcs_access,
    dag=dag,
)

# task_send_email = PythonOperator(
#     task_id='send_status_email',
#     python_callable=send_status_email,
#     trigger_rule='all_done',  # 이전 task 성공/실패 관계없이 항상 실행
#     dag=dag,
# )

# Task 의존성
task_bigquery_projects >> task_all_projects >> task_agency_reports >> task_authorize_gcs ##>> task_send_email