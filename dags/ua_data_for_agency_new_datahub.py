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
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import re
import json
import time
from googleapiclient.discovery import build

# ============= 환경 변수 또는 Airflow Variable 조회 함수 =============
def get_var(key: str, default: str | None = None) -> str:
    env_value = os.environ.get(key)
    if env_value:
        print(f"✓ 환경 변수에서 {key} 로드됨")
        return env_value

    try:
        var_value = Variable.get(key, default_var=None)
        if var_value:
            print(f"✓ Airflow Variable에서 {key} 로드됨")
            return var_value
    except Exception as e:
        print(f"⚠️  Variable.get({key}) 오류: {str(e)}")

    if default is not None:
        print(f"ℹ️  기본값으로 {key} 설정됨")
        return default

    raise ValueError(f"필수 설정 {key}을(를) 찾을 수 없습니다. "
                     f"환경 변수 또는 Airflow Variable에서 설정하세요.")

# 이메일
RECIPIENT_EMAIL = '65e43b85.joycity.com@kr.teams.ms'


# ============= 기타 상수 =============
GCS_BUCKET_NAME = 'uadata_performance_team'
TEMP_TABLE = 'datahub-478802.datahub.f_user_map_cohort_view_temp'

scopes = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]


# ============= Airflow DAG 설정 =============
default_args = {
    'owner': 'data-team',
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': [RECIPIENT_EMAIL],
}

dag = DAG(
    dag_id='ua_data_for_agency_new_datahub',
    default_args=default_args,
    description='UA Service 일일 배치 작업 (datahub view 기반, 임시 테이블 생성/삭제)',
    schedule='0 23 * * *',  # 매일 KST 08:00 (UTC 23:00)
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

if 'private_key' in credentials_info:
    credentials_info['private_key'] = credentials_info['private_key'].replace('\\n', '\n')
    print("✓ Private key 형식 수정 완료")
print()


# ============= Task 0: datahub view → 임시 물리 테이블 생성 =============
def generate_ua_data_in_bigquery(**context):
    print("=" * 80)
    print("Task 0: datahub view 기반 임시 테이블 생성 시작")
    print("=" * 80)

    bq_client = bigquery.Client.from_service_account_info(credentials_info)

    create_query = f"""
        CREATE OR REPLACE TABLE `datahub-478802.datahub.f_user_map_cohort_view_temp` AS
        with perforaw as (
        SELECT joyple_game_code, reg_datekey, app_id, 
               gcat, media_category, media, media_source, media_detail, product_category, etc_category, optim, 
               reg_country_code,geo_cam, reg_market_id, reg_os_id, os_cam,
               adset_name, ad_name, site_id, agency, target_group,
               device, setting_title, landing_title, ad_unit,mediation, init_campaign, uptdt_campaign, campaign, class,
               count(distinct if(datediff_reg = 0, auth_account_name,null)) as ru,
               sum(if(datediff_reg between 0 and 0,daily_total_rev,0))   as rev_d0,
               sum(if(datediff_reg between 0 and 1,daily_total_rev,0))   as rev_d1,
               sum(if(datediff_reg between 0 and 3,daily_total_rev,0))   as rev_d3,
               sum(if(datediff_reg between 0 and 7,daily_total_rev,0))   as rev_d7,
               sum(if(datediff_reg between 0 and 14,daily_total_rev,0))  as rev_d14,
               sum(if(datediff_reg between 0 and 30,daily_total_rev,0))  as rev_d30,
               sum(if(datediff_reg between 0 and 60,daily_total_rev,0))  as rev_d60,
               sum(if(datediff_reg between 0 and 90,daily_total_rev,0))  as rev_d90,
               sum(if(datediff_reg between 0 and 120,daily_total_rev,0)) as rev_d120,
               sum(if(datediff_reg between 0 and 150,daily_total_rev,0)) as rev_d150,
               sum(if(datediff_reg between 0 and 180,daily_total_rev,0)) as rev_d180,
               sum(if(datediff_reg between 0 and 210,daily_total_rev,0)) as rev_d210,
               sum(if(datediff_reg between 0 and 240,daily_total_rev,0)) as rev_d240,
               sum(if(datediff_reg between 0 and 270,daily_total_rev,0)) as rev_d270,
               sum(if(datediff_reg between 0 and 300,daily_total_rev,0)) as rev_d300,
               sum(if(datediff_reg between 0 and 330,daily_total_rev,0)) as rev_d330,
               sum(if(datediff_reg between 0 and 360,daily_total_rev,0)) as rev_d360,
               sum(if(datediff_reg between 0 and 390,daily_total_rev,0)) as rev_d390,
               sum(if(datediff_reg between 0 and 420,daily_total_rev,0)) as rev_d420,
               sum(if(datediff_reg between 0 and 450,daily_total_rev,0)) as rev_d450,
               sum(if(datediff_reg between 0 and 480,daily_total_rev,0)) as rev_d480,
               sum(if(datediff_reg between 0 and 510,daily_total_rev,0)) as rev_d510,
               sum(if(datediff_reg >= 0,daily_total_rev,0)) as rev_Dcum,

               sum(if(datediff_reg between 0 and 0,daily_IAA_rev,0))   as rev_iaa_d0,
               sum(if(datediff_reg between 0 and 1,daily_IAA_rev,0))   as rev_iaa_d1,
               sum(if(datediff_reg between 0 and 3,daily_IAA_rev,0))   as rev_iaa_d3,
               sum(if(datediff_reg between 0 and 7,daily_IAA_rev,0))   as rev_iaa_d7,
               sum(if(datediff_reg between 0 and 14,daily_IAA_rev,0))  as rev_iaa_d14,
               sum(if(datediff_reg between 0 and 30,daily_IAA_rev,0))  as rev_iaa_d30,
               sum(if(datediff_reg between 0 and 60,daily_IAA_rev,0))  as rev_iaa_d60,
               sum(if(datediff_reg between 0 and 90,daily_IAA_rev,0))  as rev_iaa_d90,
               sum(if(datediff_reg between 0 and 120,daily_IAA_rev,0)) as rev_iaa_d120,
               sum(if(datediff_reg between 0 and 150,daily_IAA_rev,0)) as rev_iaa_d150,
               sum(if(datediff_reg between 0 and 180,daily_IAA_rev,0)) as rev_iaa_d180,
               sum(if(datediff_reg between 0 and 210,daily_IAA_rev,0)) as rev_iaa_d210,
               sum(if(datediff_reg between 0 and 240,daily_IAA_rev,0)) as rev_iaa_d240,
               sum(if(datediff_reg between 0 and 270,daily_IAA_rev,0)) as rev_iaa_d270,
               sum(if(datediff_reg between 0 and 300,daily_IAA_rev,0)) as rev_iaa_d300,
               sum(if(datediff_reg between 0 and 330,daily_IAA_rev,0)) as rev_iaa_d330,
               sum(if(datediff_reg between 0 and 360,daily_IAA_rev,0)) as rev_iaa_d360,
               sum(if(datediff_reg between 0 and 390,daily_IAA_rev,0)) as rev_iaa_d390,
               sum(if(datediff_reg between 0 and 420,daily_IAA_rev,0)) as rev_iaa_d420,
               sum(if(datediff_reg between 0 and 450,daily_IAA_rev,0)) as rev_iaa_d450,
               sum(if(datediff_reg between 0 and 480,daily_IAA_rev,0)) as rev_iaa_d480,
               sum(if(datediff_reg between 0 and 510,daily_IAA_rev,0)) as rev_iaa_d510,
               sum(if(datediff_reg >= 0,daily_IAA_rev,0)) as rev_iaa_Dcum,

              count(distinct if(datediff_reg =1, auth_account_name,null))   as ru_d1,
              count(distinct if(datediff_reg =3, auth_account_name,null))   as ru_d3,
              count(distinct if(datediff_reg =7, auth_account_name,null))   as ru_d7,
              count(distinct if(datediff_reg =14, auth_account_name,null))  as ru_d14,
              count(distinct if(datediff_reg =30, auth_account_name,null))  as ru_d30,
              count(distinct if(datediff_reg =60, auth_account_name,null))  as ru_d60,
              count(distinct if(datediff_reg =90, auth_account_name,null))  as ru_d90,
              count(distinct if(datediff_reg =120, auth_account_name,null)) as ru_d120,
              count(distinct if(datediff_reg =150, auth_account_name,null)) as ru_d150,
              count(distinct if(datediff_reg =180, auth_account_name,null)) as ru_d180,
              count(distinct if(datediff_reg =210, auth_account_name,null)) as ru_d210,
              count(distinct if(datediff_reg =240, auth_account_name,null)) as ru_d240,
              count(distinct if(datediff_reg =270, auth_account_name,null)) as ru_d270,
              count(distinct if(datediff_reg =300, auth_account_name,null)) as ru_d300,
              count(distinct if(datediff_reg =330, auth_account_name,null)) as ru_d330,
              count(distinct if(datediff_reg =360, auth_account_name,null)) as ru_d360,
              count(distinct if(datediff_reg =390, auth_account_name,null)) as ru_d390,
              count(distinct if(datediff_reg =420, auth_account_name,null)) as ru_d420,
              count(distinct if(datediff_reg =450, auth_account_name,null)) as ru_d450,
              count(distinct if(datediff_reg =480, auth_account_name,null)) as ru_d480,
              count(distinct if(datediff_reg =510, auth_account_name,null)) as ru_d510,

              count(distinct if(datediff_reg =0 and daily_total_rev > 0 , auth_account_name,null))   as pu_d0,
              count(distinct if(datediff_reg =1 and daily_total_rev > 0 , auth_account_name,null))   as pu_d1,
              count(distinct if(datediff_reg =3 and daily_total_rev > 0 , auth_account_name,null))   as pu_d3,
              count(distinct if(datediff_reg =7 and daily_total_rev > 0 , auth_account_name,null))   as pu_d7,
              count(distinct if(datediff_reg =14 and daily_total_rev > 0 , auth_account_name,null))  as pu_d14,
              count(distinct if(datediff_reg =30 and daily_total_rev > 0 , auth_account_name,null))  as pu_d30,
              count(distinct if(datediff_reg =60 and daily_total_rev > 0 , auth_account_name,null))  as pu_d60,
              count(distinct if(datediff_reg =90 and daily_total_rev > 0 , auth_account_name,null))  as pu_d90,
              count(distinct if(datediff_reg =120 and daily_total_rev > 0 , auth_account_name,null)) as pu_d120,
              count(distinct if(datediff_reg =150 and daily_total_rev > 0 , auth_account_name,null)) as pu_d150,
              count(distinct if(datediff_reg =180 and daily_total_rev > 0 , auth_account_name,null)) as pu_d180,
              count(distinct if(datediff_reg =210 and daily_total_rev > 0 , auth_account_name,null)) as pu_d210,
              count(distinct if(datediff_reg =240 and daily_total_rev > 0 , auth_account_name,null)) as pu_d240,
              count(distinct if(datediff_reg =270 and daily_total_rev > 0 , auth_account_name,null)) as pu_d270,
              count(distinct if(datediff_reg =300 and daily_total_rev > 0 , auth_account_name,null)) as pu_d300,
              count(distinct if(datediff_reg =330 and daily_total_rev > 0 , auth_account_name,null)) as pu_d330,
              count(distinct if(datediff_reg =360 and daily_total_rev > 0 , auth_account_name,null)) as pu_d360,
              count(distinct if(datediff_reg =390 and daily_total_rev > 0 , auth_account_name,null)) as pu_d390,
              count(distinct if(datediff_reg =420 and daily_total_rev > 0 , auth_account_name,null)) as pu_d420,
              count(distinct if(datediff_reg =450 and daily_total_rev > 0 , auth_account_name,null)) as pu_d450,
              count(distinct if(datediff_reg =480 and daily_total_rev > 0 , auth_account_name,null)) as pu_d480,
              count(distinct if(datediff_reg =510 and daily_total_rev > 0 , auth_account_name,null)) as pu_d510

        FROM `datahub-478802.datahub.f_user_map_mas_view` AS a
     --    WHERE reg_datekey >= '2017-04-27'
        where reg_datekey >= '2025-01-01' and datekey >= '2025-01-01'
        and datediff_reg >= 0
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30
        ),

        UA_perfo as (
        select a.joyple_game_code, a.reg_datekey
               , a.app_id, a.media_source, a.uptdt_campaign
               , case when a.media_source in ('Unknown', 'NULL') then 'Unknown'                
                      when a.campaign like '%Pirates of the Caribbean Android AU%' then 'UA'  
                      when a.campaign like '%Pirates of the Caribbean Android KR%' then 'UA' 
                      when a.campaign like '%Pirates of the Caribbean Android US%' then 'UA'
                      when a.campaign like '%Pirates of the Caribbean Android GB%' then 'UA'  
                      when a.campaign = 'POTC_検索' then 'UA'
                      when a.gcat is null and a.joyple_game_code =131 then d.gcat 
                 else a.gcat
                 end as gcat
               , case  when a.campaign like '%Pirates of the Caribbean Android AU%' then 'ADNW'
                       when a.campaign like '%Pirates of the Caribbean Android KR%' then 'ADNW'
                       when a.campaign like '%Pirates of the Caribbean Android US%' then 'ADNW'
                       when a.campaign like '%Pirates of the Caribbean Android GB%' then 'ADNW'
                       when a.campaign = 'POTC_検索' then 'ADNW' 
                       when a.gcat is null and a.joyple_game_code = 131 then d.media_category 
                       else a.media_category 
                 end as media_category 
               , case when a.optim  = 'NONE' and a.Adset_Name like '%MAIA%' then 'MAIA'
                      when a.optim  = 'NONE' and a.Adset_Name like '%AEO%' then 'AEO'
                      when a.optim  = 'NONE' and a.Adset_Name like '%VO%' then 'VO'
                 else a.optim end as optim 
               , a.* except(joyple_game_code,reg_datekey,app_id,media_source,uptdt_campaign, gcat, media_category,optim)
        from(select a.*, b.market_name_KR as reg_market_name, o.os_name_lower as reg_os_name
             from perforaw as a
             left join `datahub-478802.datahub.dim_market_id` as b on a.reg_market_id = b.market_id
             left join `datahub-478802.datahub.dim_os_id` as o on a.reg_os_id = o.os_id
             ) as a
        left join `data-science-division-216308.POTC.before_mas_campaign` as d
        on a.uptdt_campaign = d.campaign 


        ),

        tracker as (

        select joyple_game_code
             , install_datekey as reg_datekey
             , CASE WHEN a.media_source = '' OR a.media_source IS NULL THEN 'Unknown' ELSE a.media_source END AS media_source
             , CASE WHEN a.init_campaign = '' OR a.init_campaign IS NULL THEN 'NULL' ELSE a.init_campaign     END AS init_campaign
             , CASE WHEN a.country_code = '' OR a.country_code IS NULL THEN 'NULL' ELSE a.country_code END AS reg_country_code
             , CASE WHEN c.market_name_KR= '' OR c.market_name_KR IS NULL THEN 'NULL' ELSE c.market_name_KR END AS reg_market_name
             , CASE WHEN a.adset_name   = '' OR a.adset_name    IS NULL THEN 'NULL' ELSE a.adset_name    END AS adset_name
             , CASE WHEN a.ad_name   = '' OR a.ad_name    IS NULL THEN 'NULL' ELSE a.ad_name    END AS ad_name
             , CASE WHEN a.site_id   = '' OR a.site_id    IS NULL THEN 'NULL' ELSE a.site_id    END AS site_id
             , CASE WHEN a.agency   = '' OR a.agency    IS NULL THEN 'NULL' ELSE a.agency    END AS agency
             , CASE WHEN a.app_id = '' OR a.app_id IS NULL THEN 'NULL' ELSE a.app_id END AS app_id
             , platform as reg_os_name
             , count(*) as install
        from `datahub-478802.datahub.f_tracker_first` as a
        left join `datahub-478802.datahub.dim_market_id` as c on a.market_id = c.market_id
        where install_datekey >= '2025-01-01'
        group by 1,2,3,4,5,6,7,8,9,10,11,12

        )

        , ua_install as (
        select  
              joyple_game_code            
            , reg_datekey      
            , media_source     
            , init_campaign         
            , reg_country_code 
            , reg_market_name  
            , reg_os_name      
            , adset_name       
            , ad_name          
            , app_id           
            , agency           
            , site_id
            , a.* except(joyple_game_code,reg_datekey,media_source,init_campaign,reg_country_code,reg_market_name,reg_os_name,adset_name,
                         ad_name,app_id,agency,site_id)          
            , d.install
        from(select a.* except(app_id, media_source, init_campaign, reg_country_code, adset_name, ad_name, site_id, agency, reg_market_name)

                    , CASE WHEN a.app_id = '' OR a.app_id IS NULL THEN 'NULL' ELSE a.app_id END AS app_id
                    , CASE WHEN a.media_source = '' OR a.media_source IS NULL THEN 'Unknown' ELSE a.media_source END AS media_source
                    , CASE WHEN a.init_campaign = '' OR a.init_campaign IS NULL THEN 'NULL' ELSE a.init_campaign     END AS init_campaign
                    , CASE WHEN a.reg_country_code = '' OR a.reg_country_code IS NULL THEN 'NULL' ELSE a.reg_country_code END AS reg_country_code 
                    , CASE WHEN a.reg_market_name = '' OR a.reg_market_name IS NULL THEN 'NULL' ELSE a.reg_market_name END AS reg_market_name 
                    , CASE WHEN a.adset_name   = '' OR a.adset_name    IS NULL THEN 'NULL' ELSE a.adset_name    END AS adset_name
                    , CASE WHEN a.ad_name   = '' OR a.ad_name    IS NULL THEN 'NULL' ELSE a.ad_name    END AS ad_name
                    , CASE WHEN a.site_id   = '' OR a.site_id    IS NULL THEN 'NULL' ELSE a.site_id    END AS site_id
                    , CASE WHEN a.agency   = '' OR a.agency    IS NULL THEN 'NULL' ELSE a.agency    END AS agency
             from UA_perfo as a
             ) as a

        full join tracker as d USING (joyple_game_code, reg_datekey, media_source, init_campaign, reg_country_code, reg_market_name, reg_os_name, adset_name, ad_name, app_id, agency, site_id)

        )



                SELECT
                  b.game_code_name AS project_name
                , a.joyple_game_code
                , reg_datekey AS regdate_joyple_kst
                , ifnull(a.app_id, c.app_id) as app_id
                , COALESCE(a.gcat, c.gcat, 'Unknown') as gcat
                ,CASE WHEN a.media_source = 'Organic' then 'Organic'
                      when a.media_source = 'Unknown'  then 'Unknown' 
                      when a.media_category is null then c.media_category
                      else a.media_category
                end as media_category
                ,CASE WHEN a.media_source = 'Organic' then 'Organic'
                      when a.media_source = 'Unknown'  then 'Unknown'
                      when a.media is null then c.media
                    else a.media
                end as media
                , ifnull(a.media_source, c.media_source) as media_source
                , ifnull(a.media_detail, c.media_detail) as media_detail
                , ifnull(a.product_category, c.product_category) as product_category
                , ifnull(a.etc_category, c.etc_category) as etc_category
                , ifnull(a.optim, c.optim) as optim
                , COALESCE(c.uptdt_campaign,a.init_campaign, c.init_campaign) AS campaign
                , reg_country_code AS geo
                , ifnull(a.geo_cam, c.geo_cam) as geo_cam
                , reg_market_name AS market
                , reg_os_name AS Os
                , ifnull(a.os_cam, c.os_cam) as os_cam
                , adset_name AS fb_adset_name
                , ad_name AS fb_adgroup_name
                , site_id AS af_siteid
                , agency
                , ifnull(a.device, c.device) as device
                , ifnull(a.setting_title, c.setting_title) as setting_title
                , ifnull(a.landing_title, c.landing_title) as landing_title
                , ifnull(a.ad_unit, c.ad_unit) as ad_unit
                , ifnull(a.mediation, c.mediation) as mediation
                , install, RU, rev_D0, rev_D1, rev_D3, rev_D7, rev_D14, rev_D30, rev_D60, rev_D90
                , rev_D120, rev_D150, rev_D180, rev_D210, rev_D240, rev_D270, rev_D300, rev_D330, rev_D360, rev_D390
                , rev_D420, rev_D450, rev_D480, rev_D510, rev_Dcum
                , rev_iaa_D0, rev_iaa_D1, rev_iaa_D3, rev_iaa_D7
                , rev_iaa_D14, rev_iaa_D30, rev_iaa_D60, rev_iaa_D90, rev_iaa_D120, rev_iaa_D150, rev_iaa_D180
                , rev_iaa_D210, rev_iaa_D240, rev_iaa_D270, rev_iaa_D300, rev_iaa_D330, rev_iaa_D360, rev_iaa_D390
                , rev_iaa_D420, rev_iaa_D450, rev_iaa_D480, rev_iaa_D510, rev_iaa_Dcum
                , RU_D1, RU_D3, RU_D7, RU_D14, RU_D30, RU_D60, RU_D90
                , Pu_D0, Pu_D1, Pu_D3, Pu_D7, Pu_D14, Pu_D30, Pu_D60, Pu_D90
                , Pu_D120, Pu_D150, Pu_D180, Pu_D210, Pu_D240, Pu_D270, Pu_D300, Pu_D330
                , Pu_D360, Pu_D390, Pu_D420, Pu_D450, Pu_D480, Pu_D510
                FROM ua_install as a
                left join (select if(joyple_game_code in (159,1590), 'RESU', game_code_name) as game_code_name, joyple_game_code
                           from `datahub-478802.datahub.dim_joyple_game_code`) as b
                    on a.joyple_game_code = b.joyple_game_code
                left join `datahub-478802.datahub.dim_AFC_campaign`  as c
                    on a.app_id = c.app_id and a.media_source = c.media_source and a.init_campaign = c.init_campaign
                WHERE reg_datekey BETWEEN DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 3 MONTH)
                AND DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
    """

    try:
        print("✓ 임시 테이블 생성 쿼리 실행 중...")
        result = bq_client.query(create_query)
        result.result()
        print(f"✅ 임시 테이블 생성 완료: {TEMP_TABLE}")
    except Exception as e:
        print(f"✗ 임시 테이블 생성 오류: {str(e)}")
        raise e

    print("=" * 80)
    print("✅ Task 0 완료")
    print("=" * 80)


# ============= [테스트용] Task: 전체 프로젝트 보고서 GCS 업로드 → Sheet3에 URL 저장 =============
def write_to_sheet3_for_test(**context):
    """[테스트용] Task 1과 동일한 로직으로 GCS 업로드 후 다운로드 URL을 Sheet3에 저장"""
    print("=" * 80)
    print("[테스트] 전체 프로젝트 보고서 생성 및 Sheet3 URL 저장 시작")
    print("=" * 80)

    creds = Credentials.from_service_account_info(credentials_info, scopes=scopes)
    client = gspread.authorize(creds)
    bq_client = bigquery.Client.from_service_account_info(credentials_info)
    storage_client = storage.Client.from_service_account_info(credentials_info)

    spreadsheet_url = "https://docs.google.com/spreadsheets/d/1gMEd4_sTX-Y1jr4JcZyonDiMzazKfJOjHdahvhBKNGE/edit?gid=0#gid=0"
    spreadsheet = client.open_by_url(spreadsheet_url)

    # Sheet3 A열에서 프로젝트 목록 읽기, B열에 URL 저장
    sheet3 = spreadsheet.worksheet('sheet3')
    project_list = sheet3.col_values(1)[1:]  # 1행은 헤더로 스킵

    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    today = datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d')
    two_days_ago = (datetime.now(timezone(timedelta(hours=9))) - timedelta(days=2)).strftime('%Y-%m-%d')

    failed_projects = []
    total_rows = 0

    print(f"전체 프로젝트 수: {len(project_list)}")
    print(f"프로젝트 리스트: {project_list}")

    for project_name in project_list:
        if not project_name.strip():
            continue
        try:
            query = f"""
                SELECT * EXCEPT(Pu_D0, Pu_D1, Pu_D3, Pu_D7, Pu_D14, Pu_D30, Pu_D60, Pu_D90, Pu_D120, Pu_D150, Pu_D180, Pu_D210
                              , Pu_D240, Pu_D270, Pu_D300, Pu_D330, Pu_D360, Pu_D390, Pu_D420, Pu_D450, Pu_D480, Pu_D510)
                FROM `{TEMP_TABLE}`
                WHERE
                    project_name = @project_name
                    AND regdate_joyple_kst BETWEEN DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 3 MONTH)
                                               AND DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("project_name", "STRING", project_name)]
            )
            df = bq_client.query(query, job_config=job_config).to_dataframe()

            if df.empty:
                print(f"⚠️ [WARNING] '{project_name}'에 대한 데이터가 0건입니다.")
            else:
                print(f"✅ '{project_name}' 조회 성공: {len(df)} 행")

            csv_buffer = io.BytesIO()
            df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')

            blob_name = f"all_reports/test_{project_name}_{today}.csv"
            blob = bucket.blob(blob_name)
            blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")
            print(f"✓ GCS 업로드 완료: {blob.name}")

            signed_url = blob.generate_signed_url(version="v4", expiration=timedelta(hours=24), method="GET")

            # Sheet3 A열에서 해당 project_name 행 찾아 B열에 URL 저장
            cell = sheet3.find(project_name)
            if cell:
                sheet3.update_cell(cell.row, 2, signed_url)  # B열
            print(f"✓ Sheet3 B열 URL 업데이트 완료: {project_name}")

            total_rows += len(df)

            old_blob_name = f"all_reports/test_{project_name}_{two_days_ago}.csv"
            old_blob = bucket.blob(old_blob_name)
            if old_blob.exists():
                old_blob.delete()

        except Exception as e:
            print(f"✗ [{project_name}] 오류 발생: {str(e)}")
            failed_projects.append({'project': project_name, 'error': str(e)})
            continue

    if failed_projects:
        print("\n⚠️  실패한 프로젝트 목록:")
        for f in failed_projects:
            print(f"  - {f['project']}: {f['error']}")

    print("=" * 80)
    print(f"✅ [테스트] 완료: 총 {total_rows} 행, 실패 {len(failed_projects)}건")
    print("=" * 80)


# ============= Task 1: 전체 프로젝트 보고서 생성 및 업로드 =============
# [주석 처리: 향후 서비스 코드, 테스트 시 write_to_sheet3_for_test 사용]
def generate_all_projects_reports(**context):
    """전체 프로젝트 보고서 생성 및 GCS 업로드"""
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

    print("=" * 80)
    print(f"전체 프로젝트 수: {len(project_list)}")
    print(f"프로젝트 리스트: {project_list}")
    print("=" * 80)

    for project_name in project_list:
        if not project_name.strip():
            continue
        try:
            query = f"""
                SELECT * EXCEPT(Pu_D0, Pu_D1, Pu_D3, Pu_D7, Pu_D14, Pu_D30, Pu_D60, Pu_D90, Pu_D120, Pu_D150, Pu_D180, Pu_D210
                              , Pu_D240, Pu_D270, Pu_D300, Pu_D330, Pu_D360, Pu_D390, Pu_D420, Pu_D450, Pu_D480, Pu_D510)
                FROM `{TEMP_TABLE}`
                WHERE
                    project_name = @project_name
                    AND regdate_joyple_kst BETWEEN DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 3 MONTH)
                                               AND DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("project_name", "STRING", project_name)]
            )
            df = bq_client.query(query, job_config=job_config).to_dataframe()

            if df.empty:
                print(f"⚠️ [WARNING] '{project_name}'에 대한 데이터가 0건입니다. (기간: 최근 3개월)")
            else:
                print(f"✅ '{project_name}'의 데이터 조회 성공: {len(df)} 행")

            csv_buffer = io.BytesIO()
            df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')

            blob_name = f"all_reports/{project_name}_{today}.csv"
            blob = bucket.blob(blob_name)
            blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")
            print(f"✓ GCS 업로드 완료 (한글 깨짐 방지 적용): {blob.name}")

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

    creds = Credentials.from_service_account_info(credentials_info, scopes=scopes)
    client = gspread.authorize(creds)
    bq_client = bigquery.Client.from_service_account_info(credentials_info)
    storage_client = storage.Client.from_service_account_info(credentials_info)

    spreadsheet_url = "https://docs.google.com/spreadsheets/d/1gMEd4_sTX-Y1jr4JcZyonDiMzazKfJOjHdahvhBKNGE/edit?gid=1879882444#gid=1879882444"
    spreadsheet = client.open_by_url(spreadsheet_url)
    worksheet = spreadsheet.worksheet('sheet2')

    project_list = worksheet.col_values(1)[1:]
    agency_list = worksheet.col_values(2)[1:]

    print(f"프로젝트 리스트: {project_list}")
    print(f"대행사 리스트: {agency_list}")

    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    today = datetime.today().strftime('%Y-%m-%d')
    two_days_ago = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')

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

    failed_reports = []
    success_count = 0

    for i in range(len(project_list)):
        pr_name = str(project_list[i]).strip() if i < len(project_list) else ""
        agency_name = str(agency_list[i]).strip() if i < len(agency_list) else ""

        if not pr_name or not agency_name:
            continue

        try:
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

            base_query = f"""
                SELECT {columns_to_select}
                FROM `{TEMP_TABLE}`
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

            csv_buffer = io.BytesIO()
            df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')

            blob_name = f"agency_reports/{pr_name}_{agency_name}_{today}.csv"
            blob = bucket.blob(blob_name)
            blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")
            print(f"✓ GCS 업로드 완료 (BOM 적용): {blob.name}")

            expiration = timedelta(hours=24)
            signed_url = blob.generate_signed_url(
                version="v4",
                expiration=expiration,
                method="GET"
            )

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

            old_blob_name = f"agency_reports/{pr_name}_{agency_name}_{two_days_ago}.csv"
            old_blob = bucket.blob(old_blob_name)
            if old_blob.exists():
                old_blob.delete()
                print(f"  → 이전 파일 삭제: {old_blob_name}")

        except Exception as e:
            print(f"✗ [{pr_name}, {agency_name}] 오류 발생: {str(e)}")
            failed_reports.append((pr_name, agency_name, str(e)))
            continue

    summary = {
        'status': 'success' if not failed_reports else 'partial_failed',
        'success_count': success_count,
        'total_reports': len(project_list),
        'failed_reports': failed_reports
    }
    context['ti'].xcom_push(key='agency_reports', value=summary)

    if failed_reports:
        print("\n⚠️  실패한 보고서 목록:")
        for report in failed_reports:
            print(f"  - {report['project']} / {report['agency']}: {report['error']}")

    print("=" * 80)
    print("Task 2: 대행사 보고서 생성 완료")


# ============= Task 3: 권한 부여 =============
def authorize_gcs_access(**context):
    creds = Credentials.from_service_account_info(credentials_info, scopes=scopes)
    client = gspread.authorize(creds)

    spreadsheet_url = "https://docs.google.com/spreadsheets/d/1gMEd4_sTX-Y1jr4JcZyonDiMzazKfJOjHdahvhBKNGE/edit?gid=1879882444#gid=1879882444"
    spreadsheet = client.open_by_url(spreadsheet_url)
    worksheet = spreadsheet.worksheet('sheet2')

    mail_list = worksheet.col_values(3)[1:]
    targeturl_list = worksheet.col_values(5)[1:]
    link_list = worksheet.col_values(4)[1:]

    drive_service = build('drive', 'v3', credentials=creds)
    sheets_service = build('sheets', 'v4', credentials=creds)

    for idx, (mails_str, target_url_str, link) in enumerate(zip(mail_list, targeturl_list, link_list)):
        if not mails_str or not target_url_str or not link:
            continue

        print(f"\n--- {idx + 1}번째 행 처리 ---")

        emails = [email.strip() for email in str(mails_str).split(',') if email.strip()]

        print(f"이메일: {emails}")
        print(f"타겟 URL: {target_url_str}")
        print(f"링크: {link}")

        match = re.search(r'/d/([a-zA-Z0-9-_]+)', str(target_url_str))
        if not match:
            print(f"✗ URL에서 Spreadsheet ID를 추출할 수 없습니다: {target_url_str}")
            continue

        target_file_id = match.group(1)
        print(f"타겟 파일 ID: {target_file_id}")

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

        try:
            target_sheet = client.open_by_url(str(target_url_str))
        except Exception as e:
            print(f"✗ 타겟 시트 열기 실패: {str(e)}")
            continue

        spreadsheet_id = target_sheet.id

        try:
            worksheets = target_sheet.worksheets()
            if not worksheets:
                print(f"✗ 시트가 없습니다.")
                continue
            target_sheet_title = worksheets[0].title
        except Exception as e:
            print(f"✗ 시트 정보 조회 실패: {str(e)}")
            continue

        update_data = [[link]]
        current_time = datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M:%S')

        request_body = {
            'data': [
                {
                    'range': f"{target_sheet_title}!C2",
                    'majorDimension': 'ROWS',
                    'values': update_data
                },
                {
                    'range': f"{target_sheet_title}!D2",
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
            print(f"✓ '{target_sheet_title}' 시트 데이터 업데이트 완료!")
            print(f"  업데이트 내용: {link}")
        except Exception as e:
            print(f"✗ 데이터 업데이트 실패: {str(e)}")


# ============= Task 4: 임시 테이블 삭제 =============
def cleanup_temp_table(**context):
    """메일링 서비스 완료 후 임시 물리 테이블 삭제"""
    print("=" * 80)
    print("Task 4: 임시 테이블 삭제 시작")
    print("=" * 80)

    bq_client = bigquery.Client.from_service_account_info(credentials_info)

    drop_query = f"DROP TABLE IF EXISTS `{TEMP_TABLE}`"

    try:
        result = bq_client.query(drop_query)
        result.result()
        print(f"✅ 임시 테이블 삭제 완료: {TEMP_TABLE}")
    except Exception as e:
        print(f"✗ 임시 테이블 삭제 오류: {str(e)}")
        raise e

    print("=" * 80)
    print("✅ Task 4 완료")
    print("=" * 80)


# ============= Airflow Tasks 정의 =============

task_bigquery_projects = PythonOperator(
    task_id='generate_ua_data_in_bigquery',
    python_callable=generate_ua_data_in_bigquery,
    dag=dag,
)

# [주석 처리: 향후 서비스 코드]
# task_all_projects = PythonOperator(
#     task_id='generate_all_projects_reports',
#     python_callable=generate_all_projects_reports,
#     dag=dag,
# )

# task_agency_reports = PythonOperator(
#     task_id='generate_agency_reports',
#     python_callable=generate_agency_reports,
#     dag=dag,
# )

# task_authorize_gcs = PythonOperator(
#     task_id='authorize_gcs_access',
#     python_callable=authorize_gcs_access,
#     dag=dag,
# )

# [테스트용] Sheet3 저장 Task
task_write_sheet3_test = PythonOperator(
    task_id='write_to_sheet3_for_test',
    python_callable=write_to_sheet3_for_test,
    dag=dag,
)

task_cleanup_temp = PythonOperator(
    task_id='cleanup_temp_table',
    python_callable=cleanup_temp_table,
    trigger_rule='all_done',  # 이전 task 성공/실패 무관하게 항상 정리
    dag=dag,
)

# Task 의존성 (테스트용)
task_bigquery_projects >> task_write_sheet3_test >> task_cleanup_temp

# [주석 처리: 향후 서비스 의존성]
# task_bigquery_projects >> task_all_projects >> task_agency_reports >> task_authorize_gcs >> task_cleanup_temp
