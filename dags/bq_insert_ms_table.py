from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from google.oauth2 import service_account
import os
from airflow.models import Variable
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.cloud import bigquery
import json


def get_var(key: str, default: str = None) -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)

def get_bigquery_client():
    """BigQuery 클라이언트 생성"""
    CREDENTIALS_JSON = get_var('GOOGLE_CREDENTIAL_JSON')
    
    if not CREDENTIALS_JSON:
        raise ValueError("GOOGLE_CREDENTIAL_JSON이 설정되지 않았습니다.")
    
    cred_dict = json.loads(CREDENTIALS_JSON)
    
    # ✅ private_key의 \\n을 실제 줄바꿈으로 변환
    if 'private_key' in cred_dict:
        cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
    
    credentials = service_account.Credentials.from_service_account_info(
        cred_dict,
        scopes=[
            'https://www.googleapis.com/auth/bigquery',
            'https://www.googleapis.com/auth/drive.readonly',
            'https://www.googleapis.com/auth/spreadsheets.readonly',
            ]
    )

    print(f"📧 사용 중인 서비스 계정: {cred_dict.get('client_email')}")
    
    client = bigquery.Client(
        credentials=credentials,
        project='data-science-division-216308'
    )
    
    return client


# 사용
def check_service_account():
    """서비스 계정 정보 확인 및 안내"""
    try:
        CREDENTIALS_JSON = get_var('GOOGLE_CREDENTIAL_JSON')
        cred_dict = json.loads(CREDENTIALS_JSON)
        
        email = cred_dict.get('client_email', 'Not found')
        project = cred_dict.get('project_id', 'Not found')
        
        print("=" * 80)
        print("📧 서비스 계정 이메일:")
        print(f"   {email}")
        print()
        print("📁 프로젝트 ID:")
        print(f"   {project}")
        print()
        print("⚠️  다음 단계를 수행하세요:")
        print("   1. Google Sheets를 엽니다")
        print("   2. 우측 상단 '공유' 버튼을 클릭합니다")
        print(f"   3. 위의 이메일({email})을 추가합니다")
        print("   4. 권한을 '뷰어'로 설정합니다")
        print("   5. '완료'를 클릭합니다")
        print("=" * 80)
        
        return email
        
    except Exception as e:
        print(f"❌ 확인 실패: {str(e)}")
        raise


def check_dataset_location(**context):
    """Dataset 위치 확인 및 생성"""
    client = get_bigquery_client()
    
    project_id = "data-science-division-216308"
    dataset_id = "ALL_games"
    dataset_ref = f"{project_id}.{dataset_id}"
    
    try:
        # Dataset이 존재하는지 확인
        dataset = client.get_dataset(dataset_ref)
        
        print(f"✅ Dataset 발견!")
        print(f"   Dataset ID: {dataset.dataset_id}")
        print(f"   위치(Location): {dataset.location}")
        
        return dataset.location
        
    except Exception as e:
        print(f"❌ Dataset을 찾을 수 없습니다: {str(e)}")
        
        # 프로젝트 내 모든 Dataset 나열 (수정)
        print(f"\n📂 프로젝트 내 Dataset 목록:")
        try:
            datasets = list(client.list_datasets(project_id))
            if datasets:
                for ds in datasets:
                    # ✅ DatasetListItem에서 dataset_id만 가져오기
                    print(f"   - {ds.dataset_id}")
                    # 각 dataset의 상세 정보 가져오기
                    try:
                        full_dataset = client.get_dataset(f"{project_id}.{ds.dataset_id}")
                        print(f"     위치: {full_dataset.location}")
                    except:
                        pass
            else:
                print(f"   (Dataset이 없습니다)")
        except Exception as list_error:
            print(f"   Dataset 목록 조회 실패: {str(list_error)}")
        
        # Dataset 생성
        print(f"\n📝 Dataset '{dataset_id}'를 생성합니다...")
        new_dataset = bigquery.Dataset(dataset_ref)
        new_dataset.location = "asia-northeast3"  # 원하는 리전으로 변경
        
        try:
            created_dataset = client.create_dataset(new_dataset, timeout=30)
            print(f"✅ Dataset 생성 완료!")
            print(f"   Dataset ID: {created_dataset.dataset_id}")
            print(f"   위치(Location): {created_dataset.location}")
            return created_dataset.location
        except Exception as create_error:
            print(f"❌ Dataset 생성 실패: {str(create_error)}")
            raise



bq_create_ms_table = Dataset('bq_create_ms_table')

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id='bq_insert_ms_table',
    default_args=default_args,
    description='BigQuery 테이블 생성 예시',
    schedule=[bq_create_ms_table],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['bigquery', 'create_table'],
) as dag:
    
    def Truncate_performance_creatives(**context):

        client = get_bigquery_client()

        query = """
        TRUNCATE TABLE `data-science-division-216308.ALL_games.Performance_Creatives`
        """

        job = client.query(query)
        job.result()

        print(f"✅ Performance_Creatives ✅ 테이블 TRUNCATE 완료")

    def Insert_performance_creatives(**context):

        client = get_bigquery_client()

        query = """
        INSERT INTO `data-science-division-216308.ALL_games.Performance_Creatives`
        with
        params as (
        select
            ['POTC','GBTW','WWM','DS','JTWN','BSTD','RESU'] as selected_projects,
            date('2023-01-01') as fb_start_date,
            date('2023-01-01') as google_start_date,
            current_date('Asia/Seoul') - 1 as end_date
        )

        , project_map as (
        select 131 as joyplegameid, 'POTC' as code, '1.POTC' as label
        union all select 133,    'GBTW', '2.GBTW'
        union all select 30001,  'WWM',  '3.WWM'
        union all select 30003,  'DS',   '4.DS'
        union all select 156,    'JTWN', '5.JTWN'
        union all select 90001,  'BSTD', '6.BSTD'
        union all select 159,    'RESU', '7.RESU'
        )

        , selected as (
        select m.*
        from project_map m, params p
        where m.code in unnest(p.selected_projects)
        )

        , meta as (
        with
        ua_perfo as (
            select
            a.JoypleGameID,
            a.RegdateAuthAccountDateKST,
            a.APPID,
            a.MediaSource,
            a.CamPaign,
            b.UptdtCampaign,
            case
                when a.MediaSource in ('Unknown', 'NULL') then 'Unknown'
                when a.campaign like '%Pirates of the Caribbean Android AU%' then 'UA'
                when a.campaign like '%Pirates of the Caribbean Android KR%' then 'UA'
                when a.campaign like '%Pirates of the Caribbean Android US%' then 'UA'
                when a.campaign like '%Pirates of the Caribbean Android GB%' then 'UA'
                when a.campaign = 'POTC_検索' then 'UA'
                when b.gcat is null and a.JoypleGameID = 131 then d.gcat
                else b.gcat
            end as gcat,
            a.CountryCode,
            a.MarketName,
            a.OS,
            a.AdsetName,
            a.AdName,
            a.TrackerInstallCount,
            a.RU,
            a.rev_d0, a.rev_d1, a.rev_d3, a.rev_d7, a.rev_d14, a.rev_d30, a.rev_d60,
            a.rev_d90, a.rev_d120, a.rev_d150, a.rev_d180, a.rev_d210, a.rev_d240,
            a.rev_d270, a.rev_d300, a.rev_d330, a.rev_d360, a.rev_d390, a.rev_d420, a.rev_d450,
            a.rev_d480, a.rev_d510, a.rev_dcum,
            a.ru_d1, a.ru_d3, a.ru_d7, a.ru_d14, a.ru_d30, a.ru_d60, a.ru_d90,
            a.ru_d120, a.ru_d150, a.ru_d180,
            case
                when a.campaign like '%Pirates of the Caribbean Android AU%' then 'ADNW'
                when a.campaign like '%Pirates of the Caribbean Android KR%' then 'ADNW'
                when a.campaign like '%Pirates of the Caribbean Android US%' then 'ADNW'
                when a.campaign like '%Pirates of the Caribbean Android GB%' then 'ADNW'
                when a.campaign = 'POTC_検索' then 'ADNW'
                when b.gcat is null and a.JoypleGameID = 131 then d.media_category
                else b.mediacategory
            end as mediacategory,
            b.productcategory,
            b.media,
            b.mediadetail,
            case
                when b.optim = 'NONE' and a.AdsetName like '%MAIA%' then 'MAIA'
                when b.optim = 'NONE' and a.AdsetName like '%AEO%' then 'AEO'
                when b.optim = 'NONE' and a.AdsetName like '%VO%' then 'VO'
                else b.optim
            end as optim,
            b.etccategory,
            b.OSCAM,
            b.GEOCAM,
            b.class,
            case
                when a.MediaSource = 'Unknown' then '5.Organic'
                else b.targetgroup
            end as targetgroup
            from (
            select *
            from `dataplatform-reporting.DataService.T_0420_0000_UAPerformanceRaw_V1`
            where RegdateAuthAccountDateKST >= (select fb_start_date from params)
                and RegdateAuthAccountDateKST <= (select end_date from params)
                and JoypleGameID in (select joyplegameid from selected)
            ) as a
            left join (
            select distinct *
            from `dataplatform-reporting.DataService.V_0261_0000_AFCampaignRule_V`
            ) as b
            on a.appID = b.appID and a.MediaSource = b.MediaSource and a.Campaign = b.initCampaign
            left join `data-science-division-216308.POTC.before_mas_campaign` as d
            on a.campaign = d.campaign
        )

        , cost_raw as (
            select
            joyplegameid,
            gameid,
            cmpgndate,
            gcat,
            targetgroup,
            class,
            mediacategory,
            os,
            location,
            countrycode as geouser,
            media,
            CmpgnName,
            ifnull(adsetname, 'NULL') as adsetname,
            ifnull(adname, 'NULL') as adname,
            sum(costcurrency) as cost,
            sum(costcurrencyuptdt) as cost_exclude_credit,
            sum(impressions) as impressions,
            sum(clicks) as clicks
            from `dataplatform-reporting.DataService.V_0410_0000_CostCampaignRule_V`
            where cmpgndate >= (select fb_start_date from params)
            and cmpgndate <= (select end_date from params)
            and joyplegameid in (select joyplegameid from selected)
            group by
            joyplegameid, gameid, cmpgndate,
            gcat, targetgroup, class, mediacategory, os, location, countrycode, media, CmpgnName, adsetname, adname
        )

        , final as (
            select
            ifnull(a.joyplegameid, b.joyplegameid) as joyplegameid,
            ifnull(a.RegdateAuthAccountDateKST, b.cmpgndate) as RegdateAuthAccountDateKST,
            ifnull(a.gcat, b.gcat) as gcat,
            ifnull(a.targetgroup, b.targetgroup) as targetgroup,
            ifnull(a.class, b.class) as class,
            ifnull(a.mediacategory, b.mediacategory) as mediacategory,
            ifnull(a.media, b.media) as media,
            ifnull(a.UptdtCampaign, b.CmpgnName) as campaign,
            ifnull(a.oscam, b.os) as oscam,
            ifnull(a.geocam, b.location) as geocam,
            ifnull(a.osuser, b.os) as osuser,
            ifnull(a.geouser, b.geouser) as geouser,
            ifnull(a.adsetname, b.adsetname) as adsetname,
            ifnull(a.adname, b.adname) as adname,
            b.cost,
            b.cost_exCLUDE_credit,
            b.impressions,
            b.clicks,
            a.install,
            a.ru,
            a.rev_D0, a.rev_D1, a.rev_D3, a.rev_D7, a.rev_D14, a.rev_D30, a.rev_D60,
            a.rev_D90, a.rev_D120, a.rev_D150, a.rev_D180, a.rev_D210, a.rev_D240,
            a.rev_D270, a.rev_D300, a.rev_D330, a.rev_D360, a.rev_dcum,
            ru_d1, ru_d3, ru_d7, ru_d14, ru_d30, ru_d60, ru_d90,
            ru_d120, ru_d150, ru_d180,
            date_diff(current_date('Asia/Seoul'), coalesce(a.RegdateAuthAccountDateKST, b.cmpgndate), day) as daydiff
            from (
            select
                joyplegameid,
                RegdateAuthAccountDateKST,
                gcat, targetgroup, class, mediacategory, media, UptdtCampaign, oscam, geocam,
                CountryCode as geouser,
                case when OS = 'android' then 'And' when OS = 'ios' then 'IOS' else OS end as osuser,
                AdsetName,
                adname,
                sum(TrackerInstallCount) as install,
                sum(ru) as ru,
                sum(rev_D0) as rev_D0, sum(rev_D1) as rev_D1, sum(rev_D3) as rev_D3, sum(rev_D7) as rev_D7,
                sum(rev_D14) as rev_D14, sum(rev_D30) as rev_D30, sum(rev_D60) as rev_D60, sum(rev_D90) as rev_D90,
                sum(rev_D120) as rev_D120, sum(rev_D150) as rev_D150, sum(rev_D180) as rev_D180, sum(rev_D210) as rev_D210,
                sum(rev_D240) as rev_D240, sum(rev_D270) as rev_D270, sum(rev_D300) as rev_D300, sum(rev_D330) as rev_D330,
                sum(rev_D360) as rev_D360, sum(rev_dcum) as rev_Dcum,
                sum(ru_d1) as ru_d1, sum(ru_d3) as ru_d3, sum(ru_d7) as ru_d7, sum(ru_d14) as ru_d14,
                sum(ru_d30) as ru_d30, sum(ru_d60) as ru_d60, sum(ru_d90) as ru_d90, sum(ru_d120) as ru_d120,
                sum(ru_d150) as ru_d150, sum(ru_d180) as ru_d180
            from ua_perfo
            group by
                joyplegameid, RegdateAuthAccountDateKST,
                gcat, targetgroup, class, mediacategory, media, UptdtCampaign, oscam, geocam,
                CountryCode, OS, AdsetName, adname
            ) as a
            full join cost_raw as b
            on a.joyplegameid = b.joyplegameid
            and a.RegdateAuthAccountDateKST = b.cmpgndate
            and a.gcat = b.gcat
            and a.targetgroup = b.targetgroup
            and a.class = b.class
            and a.mediacategory = b.mediacategory
            and a.media = b.media
            and a.UptdtCampaign = b.CmpgnName
            and a.oscam = b.os
            and a.geocam = b.location
            and a.geouser = b.geouser
            and a.osuser = b.os
            and a.adsetname = b.adsetname
            and a.adname = b.adname
        )

        , creative_map as (
            select * except(rn)
            from (
            select
                s.label as project,
                c.category1,
                c.category2,
                c.subcategory,
                c.ai,
                c.type,
                c.creative_number,
                c.creation_date,
                c.youtube_link,
                c.note,
                c.file_link,
                row_number() over(partition by s.label, c.creative_number) as rn
            from `data-science-division-216308.Creative.all_standard_creative_list` as c
            join selected as s
                on c.project = s.code
            )
            where rn = 1
        )

        , final2 as (
            select
            s.label as project,
            RegdateAuthAccountDateKST as regdate_joyple_kst,
            gcat,
            targetgroup as target_group,
            campaign,
            class,
            mediacategory as media_category,
            osuser as os_user,
            adname,
            cost,
            impressions,
            clicks,
            install,
            ru,
            case
                when geouser = 'KR' then '1.KR'
                when geouser = 'US' then '2.US'
                when geouser in ('UK','FR','DE','GB') then '3.WE'
                else '4.ETC'
            end as geo_user_group,
            geouser,
            geocam,
            case when daydiff <= 1 then null else rev_d1 end as rev_D1,
            case when daydiff <= 3 then null else rev_d3 end as rev_D3,
            case when daydiff <= 7 then null else rev_d7 end as rev_D7,
            case when daydiff <= 14 then null else rev_d14 end as rev_D14,
            case when daydiff <= 30 then null else rev_d30 end as rev_D30,
            rev_Dcum,
            case when daydiff <= 1 then null else ru_d1 end as ru_d1,
            case when daydiff <= 3 then null else ru_d3 end as ru_d3,
            case when daydiff <= 7 then null else ru_d7 end as ru_d7,
            case when daydiff <= 14 then null else ru_d14 end as ru_d14,
            case when daydiff <= 30 then null else ru_d30 end as ru_d30,
            daydiff
            from final as f
            join selected as s
            on f.joyplegameid = s.joyplegameid
        )

        select
            a.project,
            regdate_joyple_kst,
            target_group,
            geo_user_group,
            geouser,
            geocam,
            os_user,
            media_category,
            campaign,
            adname,
            category1,
            category2,
            subcategory,
            ai,
            type,
            youtube_link,
            creation_date,
            case when note is null then '없음' else note end as note,
            file_link,
            sum(cost) as cost,
            sum(impressions) as impressions,
            sum(clicks) as clicks,
            sum(case when daydiff > 1 then cost end) as d1_cost,
            sum(case when daydiff > 3 then cost end) as d3_cost,
            sum(case when daydiff > 7 then cost end) as d7_cost,
            sum(case when daydiff > 14 then cost end) as d14_cost,
            sum(case when daydiff > 30 then cost end) as d30_cost,
            sum(install) as install,
            sum(ru) as ru,
            sum(case when daydiff > 1 then install end) as d1_install,
            sum(case when daydiff > 3 then install end) as d3_install,
            sum(case when daydiff > 7 then install end) as d7_install,
            sum(case when daydiff > 14 then install end) as d14_install,
            sum(case when daydiff > 30 then install end) as d30_install,
            sum(rev_d1) as rev_d1,
            sum(rev_d3) as rev_d3,
            sum(rev_d7) as rev_d7,
            sum(rev_d14) as rev_d14,
            sum(rev_d30) as rev_d30,
            sum(ru_d1) as ru_d1,
            sum(ru_d3) as ru_d3,
            sum(ru_d7) as ru_d7,
            sum(ru_d14) as ru_d14,
            sum(ru_d30) as ru_d30
        from final2 as a
        left join creative_map as b
            on a.project = b.project
            and a.adname = cast(b.creative_number as string)
        where media_category = 'Facebook'
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
        )

        , google as (
        with
        asset_raw as (
            select
            cast(split(adGroupAdAssetView.adGroupAd, '/')[offset(1)] as int64) as customer_id,
            campaign_id,
            ad_group_id,
            ad_group_ad_id,
            asset_id,
            date,
            metrics.cost_micros / 1000000 as cost,
            metrics.impressions,
            metrics.clicks,
            metrics.all_conversions,
            metrics.conversions,
            metrics.biddable_app_install_conversions,
            metrics.biddable_app_post_install_conversions
            from `dataplatform-bdts.ads_google.google_ad_group_ad_asset_view`
            where date between (select google_start_date from params) and (select end_date from params)
        )

        , google_ads_raw as (
            select distinct
            customer.id as customer_id,
            game_id as project_code,
            campaign.name as campaign_name,
            campaign.id as campaign_id,
            ad_group.id as ad_group_id,
            ad_group.name as ad_group_name
            from `dataplatform-bdts.ads_google.google_ads`
        )

        , asset_detail_raw as (
            select
            customer_id,
            asset.id as asset_id,
            asset.name as asset_name,
            asset.youtube_video_id,
            asset.asset_type
            from `dataplatform-bdts.ads_google.google_asset_details`
        )

        , asset_final as (
            select
            s.label as project,
            b.project_code,
            b.campaign_name,
            case
                when split(b.campaign_name, '_')[offset(0)] = 'KR' then '1.KR'
                when split(b.campaign_name, '_')[offset(0)] = 'US' then '2.US'
                when split(b.campaign_name, '_')[offset(0)] = 'WE' then '3.WE'
                else '4.ETC'
            end as country_cat,
            split(b.campaign_name, '_')[offset(0)] as geo_cam,
            case
                when campaign_name like '%And%' then 'And'
                when campaign_name like '%IOS%' then 'IOS'
                else 'ETC'
            end as os,
            case
                when campaign_name like '%tROAS%' then '1.UA-HVU'
                when campaign_name like '%tCPA%' then '2.UA-VU'
                when campaign_name like '%tCPI%' then '3.UA-Install'
                else '4.ETC'
            end as target_group,
            b.ad_group_id,
            b.ad_group_name,
            c.asset_type,
            case
                when c.asset_type = 'IMAGE' then regexp_replace(c.asset_name, r'\.[a-zA-Z0-9]+$', '')
                when c.asset_type = 'YOUTUBE_VIDEO' then c.youtube_video_id
                else null
            end as asset_name,
            date,
            sum(cost) as cost,
            sum(impressions) as impressions,
            sum(clicks) as clicks,
            sum(all_conversions) as all_conversions,
            sum(biddable_app_install_conversions) as installs,
            sum(biddable_app_post_install_conversions) as biddable_app_post_install_conversions
            from asset_raw as a
            left join google_ads_raw as b
            on a.customer_id = b.customer_id
            and a.campaign_id = b.campaign_id
            and a.ad_group_id = b.ad_group_id
            left join asset_detail_raw as c
            on a.customer_id = c.customer_id
            and a.asset_id = c.asset_id
            join selected s
            on b.project_code = s.code
            group by 1,2,3,4,5,6,7,8,9,10,11,12
        )

        select
            project, regdate_joyple_kst, target_group, geo_user_group, geo_cam, os_user, campaign,
            asset_name, adname, ai, category1, category2, subcategory, creation_date, youtube_link, note, file_link, type,
            sum(cost) as cost,
            sum(impressions) as impressions,
            sum(clicks) as clicks,
            sum(install) as install
        from (
            select
            project, regdate_joyple_kst, target_group, geo_user_group, geo_cam, os_user, campaign,
            first_value(asset_name ignore nulls) over(partition by project, adname order by regdate_joyple_kst nulls last rows between unbounded preceding and unbounded following) as asset_name,
            first_value(adname ignore nulls) over(partition by project, adname order by regdate_joyple_kst nulls last rows between unbounded preceding and unbounded following) as adname,
            first_value(ai ignore nulls) over(partition by project, adname order by regdate_joyple_kst nulls last rows between unbounded preceding and unbounded following) as ai,
            first_value(category1 ignore nulls) over(partition by project, adname order by regdate_joyple_kst nulls last rows between unbounded preceding and unbounded following) as category1,
            first_value(category2 ignore nulls) over(partition by project, adname order by regdate_joyple_kst nulls last rows between unbounded preceding and unbounded following) as category2,
            first_value(subcategory ignore nulls) over(partition by project, adname order by regdate_joyple_kst nulls last rows between unbounded preceding and unbounded following) as subcategory,
            first_value(creation_date ignore nulls) over(partition by project, adname order by regdate_joyple_kst nulls last rows between unbounded preceding and unbounded following) as creation_date,
            first_value(youtube_link ignore nulls) over(partition by project, adname order by regdate_joyple_kst nulls last rows between unbounded preceding and unbounded following) as youtube_link,
            first_value(note ignore nulls) over(partition by project, adname order by regdate_joyple_kst nulls last rows between unbounded preceding and unbounded following) as note,
            first_value(file_link ignore nulls) over(partition by project, adname order by regdate_joyple_kst nulls last rows between unbounded preceding and unbounded following) as file_link,
            first_value(type ignore nulls) over(partition by project, adname order by regdate_joyple_kst nulls last rows between unbounded preceding and unbounded following) as type,
            cost, impressions, clicks, install
            from (
            select
                a.project,
                date as regdate_joyple_kst,
                a.target_group,
                a.country_cat as geo_user_group,
                a.geo_cam as geo_cam,
                a.os as os_user,
                a.campaign_name as campaign,
                a.asset_name as asset_name,
                cast(coalesce(b.adname, c.adname, d.adname) as string) as adname,
                coalesce(b.ai, c.ai, d.ai) as ai,
                coalesce(b.category1, c.category1, d.category1) as category1,
                coalesce(b.category2, c.category2, d.category2) as category2,
                coalesce(b.subcategory, c.subcategory, d.subcategory) as subcategory,
                coalesce(b.creation_date, c.creation_date, d.creation_date) as creation_date,
                coalesce(b.youtube_link, c.youtube_link, d.youtube_link) as youtube_link,
                coalesce(b.note, c.note, d.note, '없음') as note,
                coalesce(b.file_link, c.file_link, d.file_link) as file_link,
                case
                when asset_type = 'IMAGE' then 'Image'
                when asset_type = 'YOUTUBE_VIDEO' then 'Video'
                else 'ETC'
                end as type,
                sum(cost) as cost,
                sum(impressions) as impressions,
                sum(clicks) as clicks,
                sum(installs) as install
            from (
                select * except(asset_name),
                case
                    when regexp_contains(asset_name, r'_\d+x\d+_\d+_\d+') then regexp_extract(asset_name, r'^(.+?_\d+x\d+_\d+_\d+)')
                    else asset_name
                end as asset_name
                from asset_final
            ) as a
            left join (
                select distinct
                project as project_code,
                category1,
                category2,
                subcategory,
                type,
                ai,
                case when type = 'Image' then file_name else video_id end as file_name,
                creative_number as adname,
                creation_date,
                note,
                youtube_link,
                file_link
                from `data-science-division-216308.Creative.all_standard_creative_list`
                where project != 'GBTW_Old'
                and project in (select code from selected)
            ) as b
                on a.asset_name = b.file_name
                and a.project_code = b.project_code
            left join (
                select distinct
                project as project_code,
                category1,
                category2,
                subcategory,
                type,
                ai,
                past_name,
                creative_number as adname,
                creation_date,
                note,
                youtube_link,
                file_link
                from `data-science-division-216308.Creative.all_standard_creative_list`
                where project != 'GBTW_Old'
                and project in (select code from selected)
            ) as c
                on a.asset_name = c.past_name
                and a.project_code = c.project_code
            left join (
                select distinct
                project as project_code,
                category1,
                category2,
                subcategory,
                type,
                ai,
                case when type = 'Image' then file_name else video_id end as file_name,
                creative_number as adname,
                creation_date,
                note,
                youtube_link,
                file_link
                from `data-science-division-216308.Creative.all_standard_creative_list`
                where project = 'GBTW_Old'
            ) as d
                on concat(a.project_code, '_Old') = d.project_code
                and a.asset_name = d.file_name
            group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
            )
        )
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
        )

        , temp1 as (
        select
            project,
            regdate_joyple_kst,
            target_group,
            geo_user_group,
            geouser,
            geocam,
            os_user,
            media_category,
            campaign,
            type,
            ai,
            category1,
            category2,
            subcategory,
            creation_date,
            adname,
            note,
            youtube_link,
            file_link,
            cost,
            impressions,
            clicks,
            install,
            ru,
            d1_cost,
            d3_cost,
            d7_cost,
            d14_cost,
            d30_cost,
            d1_install,
            d3_install,
            d7_install,
            d14_install,
            d30_install,
            rev_d1,
            rev_d3,
            rev_d7,
            rev_d14,
            rev_d30,
            ru_d1,
            ru_d3,
            ru_d7,
            ru_d14,
            ru_d30
        from meta

        union all

        select
            project,
            regdate_joyple_kst,
            target_group,
            geo_user_group,
            '구글-구분불가' as geouser,
            geo_cam,
            os_user,
            'Google' as media_category,
            campaign,
            type,
            ai,
            category1,
            category2,
            subcategory,
            creation_date,
            adname,
            note,
            youtube_link,
            file_link,
            cost,
            impressions,
            clicks,
            install,
            null as ru,
            null as d1_cost,
            null as d3_cost,
            null as d7_cost,
            null as d14_cost,
            null as d30_cost,
            null as d1_install,
            null as d3_install,
            null as d7_install,
            null as d14_install,
            null as d30_install,
            null as rev_d1,
            null as rev_d3,
            null as rev_d7,
            null as rev_d14,
            null as rev_d30,
            null as ru_d1,
            null as ru_d3,
            null as ru_d7,
            null as ru_d14,
            null as ru_d30
        from google
        )

        , temp1_link as (
        select
            *,
            case
            when type = 'Video' then coalesce(youtube_link, file_link)
            else file_link
            end as link
        from temp1
        )

        , temp1_unified as (
        select
            * except(link),
            first_value(link ignore nulls) over (
            partition by project, adname
            order by regdate_joyple_kst nulls last rows between unbounded preceding and unbounded following
            ) as link
        from temp1_link
        )

        select project, regdate_joyple_kst, target_group, geo_user_group, geouser, geocam, os_user, media_category, campaign, type, ai, category1, category2, subcategory, creation_date, adname, note, youtube_link, file_link, cost, impressions, clicks, install, ru, d1_cost, d3_cost, d7_cost, d14_cost, d30_cost, d1_install, d3_install, d7_install, d14_install, d30_install, rev_d1, rev_d3,
        rev_d7, rev_d14, rev_d30, ru_d1, ru_d3, ru_d7, ru_d14, ru_d30, link
        from temp1_unified
    """
        job = client.query(query)
        job.result()

        print(f"✅ Performance_Creatives ✅ 테이블 INSERT 완료")


    RECIPIENT_EMAIL = '65e43b85.joycity.com@kr.teams.ms'
    SMTP_HOST = get_var("SMTP_HOST", "smtp.gmail.com")
    SMTP_PORT = int(get_var("SMTP_PORT", "587"))
    SMTP_USER = get_var("SMTP_USER", RECIPIENT_EMAIL)
    SMTP_PASSWORD = get_var("SMTP_PASSWORD", "ciucjokomvmipxej")
    EMAIL_FROM = get_var("EMAIL_FROM", SMTP_USER)

    def send_status_email(**context):
        """작업 완료 후 간단한 알림 이메일 발송"""
        print("=" * 80)
        print("Task 3: 완료 알림 이메일 발송 시작")
        print("=" * 80)
        
        # 실행 날짜 (logical_date 사용)
        logical_date = context['logical_date']
        execution_date = logical_date.strftime('%Y-%m-%d %H:%M:%S KST')
        
        # 이메일 제목 및 본문 작성
        subject = "✅ MS Performance Creatives 데이터 인입 완료"
        body = f"""
    Performance Creatives 데이터 인입 완료되었습니다.

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



    # 먼저 서비스 계정 확인
    check_account = PythonOperator(
        task_id='check_service_account',
        python_callable=check_service_account,
    )

    check_dataset = PythonOperator(
        task_id='check_dataset_location',
        python_callable=check_dataset_location,
    )

    task_truncate_performance_creatives = PythonOperator(
        task_id='Truncate_performance_creatives',
        python_callable=Truncate_performance_creatives,
    )

    tast_insert_performance_creatives = PythonOperator(
        task_id='Insert_performance_creatives',
        python_callable=Insert_performance_creatives,
    )

    task_send_email = PythonOperator(
        task_id='send_status_email',
        python_callable=send_status_email,
        trigger_rule='all_done',  # 이전 task 성공/실패 관계없이 항상 실행
        dag=dag,
    )

    check_account >> check_dataset >> task_truncate_performance_creatives >> tast_insert_performance_creatives >> task_send_email