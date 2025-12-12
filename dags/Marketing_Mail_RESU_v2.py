from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery
from google.auth.transport.requests import Request
import google.auth
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import pandas as pd
import os
from airflow.models import Variable
import html

from google.genai import Client
from google.genai import types


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
    dag_id='Marketing_Mailing_RESU_v2',
    default_args=default_args,
    description='RESU 마케팅 결과를 메일링',
    schedule='01 5 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['marketing', 'mailing', 'RESU'],
) as dag:

    logger = logging.getLogger(__name__)

    def get_var(key: str, default: str = None) -> str:
        """환경 변수 또는 Airflow Variable 조회"""
        return os.environ.get(key) or Variable.get(key, default_var=default)

    # 환경 변수 설정
    PROJECT_ID = "data-science-division-216308"
    CREDENTIALS_JSON = get_var('GOOGLE_CREDENTIAL_JSON')
    
    # SMTP 설정
    # SMTP_SERVER = get_var('SMTP_SERVER', 'smtp.gmail.com')
    # SMTP_PORT = int(get_var('SMTP_PORT', '587'))
    SMTP_SERVER = "61.43.45.137"
    SMTP_PORT = 25
    SENDER_EMAIL = 'ds_bi@joycity.com'
    SENDER_PASSWORD = get_var('SMTP_PASSWORD')

    # 수신자 설정
    RECIPIENT_EMAILS = ['nayoonkim@joycity.com']
    # RECIPIENT_EMAILS = [email.strip() for email in get_var('RECIPIENT_EMAILS', '').split(',') if email.strip()]

    # 제미나이 설정
    LOCATION = "us-central1"
    PROJECT_ID = "data-science-division-216308"
    MODEL_NAME = "gemini-2.5-flash"
    LABELS = {"datascience_division_service": 'marketing_mailing'}
    SYSTEM_INSTRUCTION = """
    너는 전문 마케팅 데이터 분석가야.
    주어진 ROAS 데이터와 퍼포먼스팀의 원문 리포트를 **절대 오류 없이 분석**하고, 요청된 **모든 출력 형식 규칙**을 엄격하게 준수하여 리포트를 작성해야해.

    [데이터 정합성 최우선 규칙]
    1. 모든 수치 비교 (cost, install ru, CPI, cpru, 증감률 계산)는 오직 제공된 테이블 데이터만을 기반으로 수행해
    2. **분석에 필요한 모든 지표(Cost, CPI, CPRU, D1LTV, D1RET, D1ROAS 등)는 제공된 데이터 테이블 내에 존재하는 것만을 분석하며, 지표가 없어서 못한다는 언급은 절대 하지마.**
    3. 테이블에 없는 데이터나 추론은 엄금하며, 비교 대상은 동일한 게임 내에서 서로 다른 시점(월)의 동일한 지표(열)이야
    4. 동일한 지표(열) 내에서 “서로 다른 날짜 간 비교”만 허용되며, 서로 다른 지표끼리 비교하지 마

    [표기법 규칙]
    - cost, install ru, CPI, cpru는 천단위 쉼표(,)를 사용
    - ROAS 관련 지표는 소수점 첫째 자리까지 표기하고 '%' 단위를 사용
    - 증감률을 이야기할 때는 +- 기호 대신 🔺(상승) 또는 🔻(하락) 기호를 숫자앞에 사용해줘

    [출력형식 규칙]
    - 리포트 작성 완료했다는 내용은 별도로 언급하지마
    - 마크다운 포맷: 노션 마크다운 포맷을 사용해
    - **첫 번째 문장:** 리포트의 가장 첫 문장은 **데이터 Country 컬럼의 값**을 명시하여 시작해야 합니다.
    - 한 문장마다 시작은 # 로 시작해줘. e.g. # 당월 매출은 이렇습니다.
    - 습니다. 체로 써줘
    - 명확하고 간결하게 작성해줘
    """
    
    prompt_description = """
    ## 데이터 설명
    최근 2주간 마케팅으로 유입된 지역별, OS별 데이터야
    기간 내 지표 변화와 효율 변동을 분석하는 것이 목적이야.
    NA, Null인 Cohort변수(dn_roas)는 아직 mature되지 않은 지표야. 해당 지표에 대해서는 언급하지마.
    
    ### [기본 유입 지표]
    - **Date**: 해당 데이터가 집계된 날짜
    - **Country**: 국가 코드 또는 전체 유저 그룹 정보
    - **Cost**: 해당 날짜의 마케팅 집행 비용
    - **Install**: 해당 날짜의 전체 신규 유입 수
    - **RU : 신규 유저수
    - **Organic_ratio**: 전체 유입 중 유기적 트래픽이 차지하는 비율 (OS별 비교에서는 제외됨)

    ### [단가 지표]
    - **CPI (Cost per Install)**: Install 1건당 비용
    - **CPRU (Cost per Revenue User)**: Revenue User 1명당 비용

    ### [LTV 지표]
    각 단위는 *유저 1인당 매출 기여도*를 의미하며, 기간별 누적 LTV를 포함해.
    - **D0LTV**: 첫날 LTV
    - **D1LTV**: 1일차 LTV
    - **D3LTV / D7LTV**: 3일차 / 7일차 LTV
    - **DcumLTV**: 전체 누적 LTV (최대 기간까지)

    ### [Retention 지표]
    - **D1RET / D3RET / D7RET**: 각각 1·3·7일차 잔존율(리텐션)

    ### [ROAS 지표]
    - **D0ROAS / D1ROAS / D3ROAS / D7ROAS**: 해당 시점의 ROAS
    - **DcumROAS**: 누적 ROAS
    """
    
    prompt_part = """
    ## 마케팅 성과 분석 요청 및 규칙
    주어진 데이터의 Country/OS 그룹에 대해 아래 3가지 항목을 분석하여 문장 형태로 출력해줘.

    * **핵심 분석 규칙:**
        * **첫 문장 시작:** 첫 번째 문장은 **데이터 테이블의 첫 번째 행 Country이 존재할 경우 명시된 그룹 이름**으로 시작해야 합니다. (예: # 4.ETC 지역의 최근 2주간 마케팅 성과를 분석했습니다.)
        * **날짜 명시:** 모든 수치를 언급할 때는 비교 대상이 되는 날짜를 함께 명시해야 합니다.
        * **제외 지표:** DcumLTV, DcumROAS 및 NA 값인 지표는 언급하지 않습니다.
        * **줄 수 제한:** 총 5줄 미만으로 간결하게 작성합니다.

    * **분석 항목:**
        1)  **Cost 변화 요약** (마지막일 기준 상승/하락 여부와 증감 비율 포함)
        2)  **CPI, CPRU 변화율 중심 설명** (마지막일 기준, CPI/CPRU 각각의 증감 비율 포함)
        3)  **D1LTV, D1RET, D1ROAS 변화** (값이 존재하는 데이터 중 가장 큰 변화 1개에 대해 언급)
"""
    

    # 제미나이 paid 국가별 함수
    def genai_paid_geo_analytics(df):
        genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)
        response_data = genai_client.models.generate_content(
            model=MODEL_NAME,
            contents = [prompt_description,  prompt_part,  f"""
                        <최근 2주간 geo_user_group별 마케팅으로 유입된 유저 데이터>
                        {df}"""],
            config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_INSTRUCTION,
                    # tools=[RAG],
                    temperature=0.5,
                    labels=LABELS
                )
            )
        
        text = response_data.text
        first_hash_removed = text.replace('#', '', 1)
        return first_hash_removed.replace('#', '<br>\n*')
    

    # 제미나이 organic 국가별 함수
    def genai_organic_geo_analytics(df):
        genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)
        response_data = genai_client.models.generate_content(
            model=MODEL_NAME,
            contents = [prompt_description,  prompt_part,  f"""
                        <최근 2주간 geo_user_group별 Organic으로 유입된 유저 데이터>
                        {df}"""],
            config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_INSTRUCTION,
                    # tools=[RAG],
                    temperature=0.5,
                    labels=LABELS
                )
            )
        
        text = response_data.text
        first_hash_removed = text.replace('#', '', 1)
        return first_hash_removed.replace('#', '<br>\n*')


    # 제미나이 Paid 전체 요약 함수
    def genai_paid_all_analytics(df, text_data):
        genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)
        response_data = genai_client.models.generate_content(
            model=MODEL_NAME,
            contents = [prompt_description,  prompt_part,  f"""
                        2가지 데이터가 있어. 
                        하나는 마케팅으로 유입된 전체 Paid 유저에 대한 데이터고, 
                        이를 국가별로 확인 후 분석한 제미나이의 코멘트를 정리한 데이터야.
                        통합 데이터에 대해서 언급 한 후, 제미나이 코멘트 확인 후 가장 변화가 큰 국가의 트렌드를 아래에 적어줘.
                        
                        <마케팅으로 유입된 Paid 전체 유저 데이터>
                        {df}
                        
                        <제미나이 코멘트>
                        {text_data}
                        """],
            config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_INSTRUCTION,
                    # tools=[RAG],
                    temperature=0.5,
                    labels=LABELS
                )
            )
        
        text = response_data.text
        first_hash_removed = text.replace('#', '', 1)
        return first_hash_removed.replace('#', '<br>\n*')

    # 제미나이 전체 유저 요약 함수
    def genai_organic_all_analytics(df, text_data):
        genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)
        response_data = genai_client.models.generate_content(
            model=MODEL_NAME,
                        contents = [prompt_description,  prompt_part,  f"""
                        2가지 데이터가 있어. 
                        일자별 유입된 전체 유저에 대한 데이터고, 
                        이를 국가별로 확인 후 분석한 제미나이의 코멘트를 정리한 데이터야.
                        통합 데이터에 대해서 언급 한 후, 제미나이 코멘트 확인 후 가장 변화가 큰 국가의 트렌드를 아래에 적어줘.
                        
                        <일자별 유입된 전체 유저 데이터>
                        {df}
                        
                        <제미나이 코멘트>
                        {text_data}
                        """],
            config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_INSTRUCTION,
                    # tools=[RAG],
                    temperature=0.5,
                    labels=LABELS
                )
            )
        
        text = response_data.text
        first_hash_removed = text.replace('#', '', 1)
        return first_hash_removed.replace('#', '<br>\n*')


    # GCP 인증
    cred_dict = json.loads(CREDENTIALS_JSON)
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    credentials.refresh(Request())
    bigquery_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

    # 날짜 가져오기 
    kst = timezone(timedelta(hours=9))
    today = datetime.now(kst).date()
    two_weeks_ago = today - timedelta(days=14)
    yesterday = today - timedelta(days=1)
    
    # Basic query
    basic_query = f"""
            with UA_perfo as (
            select a.JoypleGameID, a.RegdateAuthAccountDateKST, a.APPID,
                a.MediaSource, a.CamPaign
                , b.UptdtCampaign
                , case when a.MediaSource in ('Unknown', 'NULL') then 'Unknown'                
                        when a.campaign like '%Pirates of the Caribbean Android AU%' then 'UA'  
                        when a.campaign like '%Pirates of the Caribbean Android KR%' then 'UA' 
                        when a.campaign like '%Pirates of the Caribbean Android US%' then 'UA'
                        when a.campaign like '%Pirates of the Caribbean Android GB%' then 'UA'  
                        when a.campaign = 'POTC_検索' then 'UA'
                        when b.gcat is null and a.JoypleGameID =131 then d.gcat 
                    else b.gcat
                    end as gcat
                , a.CountryCode, a.MarketName, a.OS, a.AdsetName, a.AdName
                , a.TrackerInstallCount, a.RU
                , a.rev_d0, a.rev_d1, a.rev_d3, a.rev_d7, a.rev_dcum
                , a.ru_d1, a.ru_d3, a.ru_d7
                , case  when a.campaign like '%Pirates of the Caribbean Android AU%' then 'ADNW'
                        when a.campaign like '%Pirates of the Caribbean Android KR%' then 'ADNW'
                        when a.campaign like '%Pirates of the Caribbean Android US%' then 'ADNW'
                        when a.campaign like '%Pirates of the Caribbean Android GB%' then 'ADNW'
                        when a.campaign = 'POTC_検索' then 'ADNW' 
                        when b.gcat is null and a.JoypleGameID = 131 then d.media_category 
                        else b.mediacategory 
                    end as mediacategory 
                , b.productcategory, b.media, b.mediadetail
                , case when b.optim  = 'NONE' and a.AdsetName like '%MAIA%' then 'MAIA'
                        when b.optim  = 'NONE' and a.AdsetName like '%AEO%' then 'AEO'
                        when b.optim  = 'NONE' and a.AdsetName like '%VO%' then 'VO'
                    else b.optim end as optim 
                , b.etccategory,  b.OSCAM, b.GEOCAM      
                , b.class
            , case when  a.MediaSource    = 'Unknown' then '5.Organic' else b.targetgroup end as targetgroup 
            , case when CountryCode = 'US' then '1.US'
                when CountryCode = 'JP' then '2.JP'
                when CountryCode in ('UK','FR','DE','GB') then '3.WEU'
                else '4.ETC' end as geo_user_group 
            from(select *
                from `dataplatform-reporting.DataService.T_0420_0000_UAPerformanceRaw_V1`
                where JoypleGameID in (1590,159)
                and RegdateAuthAccountDateKST >= '2025-11-18'
                and RegdateAuthAccountDateKST < CURRENT_DATE('Asia/Seoul')
                ) as a
            left join (select distinct *
                    from `dataplatform-reporting.DataService.V_0261_0000_AFCampaignRule_V`) as b
            on a.appID = b.appID and a.MediaSource = b.MediaSource and a.Campaign = b.initCampaign
            left join `data-science-division-216308.POTC.before_mas_campaign` as d
            on a.campaign = d.campaign 
            )

            , cost_raw AS(
            select joyplegameid,gameid,  cmpgndate, gcat ,mediacategory, os, geo_user_group
            , sum(costcurrency) as cost, sum(costcurrencyuptdt) as cost_exclude_credit
            from (select  * , case when CountryCode = 'US' then '1.US'
                when CountryCode = 'JP' then '2.JP'
                when CountryCode in ('UK','FR','DE','GB') then '3.WEU'
                else '4.ETC' end as geo_user_group 
            from  `dataplatform-reporting.DataService.V_0410_0000_CostCampaignRule_V`
            where joyplegameid in (1590,159)
            and cmpgndate >='2025-11-18'
                and cmpgndate < CURRENT_DATE('Asia/Seoul')
            ) 
            group by  joyplegameid,gameid,  cmpgndate, gcat, mediacategory, os,  geo_user_group
            )

            , final AS(
            select 
            ifnull(a.joyplegameid , b.joyplegameid) as joyplegameid 
            ,ifnull(a.RegdateAuthAccountDateKST , b.cmpgndate) as RegdateAuthAccountDateKST
            , ifnull(a.gcat, b.gcat) as gcat 
            , ifnull(a.mediacategory, b.mediacategory) as mediacategory
            , ifnull(a.osuser, b.os) as osuser 
            , ifnull(a.geo_user_group, b.geo_user_group) as geo_user_group 
            , a.install, a.ru 
            ,a.rev_D0, a.rev_D1, a.rev_D3, a.rev_D7, a.rev_dcum
            , ru_d1, ru_d3, ru_d7
            , b.cost, b.cost_exclude_credit
            , date_diff(  CURRENT_DATE('Asia/Seoul'), (case when a.RegdateAuthAccountDateKST is null then b.cmpgndate else a.RegdateAuthAccountDateKST end) ,day) as daydiff 
            from(
            select joyplegameid , RegdateAuthAccountDateKST, gcat, mediacategory, geo_user_group
            , case when OS = 'android' then 'And' when OS = 'ios' then 'IOS' else OS end as osuser 
            , sum(TrackerInstallCount) as install, sum(ru) as ru , sum(rev_D0) as rev_D0 ,
            sum(rev_D1) as rev_D1 , sum(rev_D3) as rev_D3 , sum(rev_D7) as rev_D7,  sum(rev_dcum) as rev_Dcum 
            , sum(ru_d1) as ru_d1, sum(ru_d3) as ru_d3, sum(ru_d7) as ru_d7
            from ua_perfo 
            group by  joyplegameid, RegdateAuthAccountDateKST, gcat, mediacategory,  geo_user_group  , os

            ) as a 
            full join cost_raw as b 
            on a.joyplegameid = b.joyplegameid
            and a.regdateauthaccountdatekst = b.cmpgndate
            and a.gcat = b.gcat 
            and a.mediacategory = b.mediacategory 
            and a.geo_user_group = b.geo_user_group 
            and a.osuser = b.os
            )


            , final2 AS(
            select joyplegameid, RegdateAuthAccountDateKST as regdate_joyple_kst , gcat, mediacategory as media_category , geo_user_group, osuser,install, ru, rev_d0, 
            case when daydiff <= 1 then null else rev_d1 end as rev_D1, 
            case when daydiff <= 3 then null else rev_d3 end as rev_D3, 
            case when daydiff <= 7 then null else rev_d7 end as rev_D7,
            rev_Dcum, 
            case when daydiff <= 1 then null else ru_d1 end as ru_d1, 
            case when daydiff <= 3 then null else ru_d3 end as ru_d3, 
            case when daydiff <= 7 then null else ru_d7 end as ru_d7,
            cost, cost_exclude_credit, 
            daydiff 
            from final)

            """

    # 숫자 포맷팅 함수 (1000단위 쉼표 추가)
    def format_number(value):
        """숫자에 1000단위 쉼표 추가 + HTML 이스케이프"""
        if pd.isna(value):
            return ''
        try:
            # 숫자 타입 확인
            num = float(value)
            # 정수인 경우
            if num == int(num):
                formatted = f"{int(num):,}"
            # 소수점이 있는 경우
            else:
                formatted = f"{num:,.2f}"
        except (ValueError, TypeError):
            formatted = str(value)
        
        # HTML 엔티티로 변환
        formatted = formatted.replace('.', '.\u200b')
        return html.escape(formatted)
        
    # HTML 표 생성 함수
    def format_table(df):
        html_table_header = '<tr class="data-title">'
        for col in df.columns:
            html_table_header += f'<td>{col}</td>'  
        html_table_header += '</tr>'
        html_table_rows = ''
        for idx, row in df.iterrows():
            row_class = 'data1' if idx % 2 == 0 else 'data2'
            html_table_rows += f'<tr class="{row_class}">'
            for cell in row:
                cell_value = format_number(cell)
                html_table_rows += f'<td>{cell_value}</td>'
            html_table_rows += '</tr>'
        return html_table_header, html_table_rows

    # 쿼리 실행 및 이메일 발송 함수
    def extract_and_send_email(**context):
        """쿼리 실행 및 이메일 발송"""
        try:
            # BigQuery 쿼리 실행
            query = basic_query + f"""
            select regdate_joyple_kst as Date --, geo_user_group 
            , CAST(sum(cost_exclude_credit) AS INT64) as Cost
            , ROUND(sum(install), 2) as Install
            , ROUND(sum(ru), 2) as Ru
            --, ROUND(SUM(CASE WHEN gcat = "Organic" or gcat = "Unknown" then ru end) / sum(ru), 2) as Organic_ratio
            , ROUND(sum(cost_exclude_credit)/sum(install), 2) as CPI 
            , ROUND(sum(cost_exclude_credit)/sum(ru), 2)  as CPRU
            , ROUND(sum(rev_d0)/sum(ru), 2)  as D0LTV
            , ROUND(sum(rev_d1)/sum(ru), 2)  as D1LTV
            , ROUND(sum(rev_d3)/sum(ru), 2)  as D3LTV
            , ROUND(sum(rev_d7)/sum(ru), 2)  as D7LTV
            , ROUND(sum(rev_dcum)/sum(ru), 2)  as DcumLTV
            , ROUND(sum(ru_d1)/sum(ru)*100, 2)  as D1RET
            , ROUND(sum(ru_d3)/sum(ru)*100, 2)  as D3RET
            , ROUND(sum(ru_d7)/sum(ru)*100, 2)  as D7RET
            , ROUND(sum(rev_d0)/sum(cost_exclude_credit)*100, 2)  as D0ROAS
            , ROUND(sum(rev_d1)/sum(cost_exclude_credit)*100, 2)  as D1ROAS
            , ROUND(sum(rev_d3)/sum(cost_exclude_credit)*100, 2)  as D3ROAS
            , ROUND(sum(rev_d7)/sum(cost_exclude_credit)*100, 2)  as D7ROAS
            , ROUND(sum(rev_dcum)/sum(cost_exclude_credit)*100, 2)  as DcumROAS
            from final2 
            where regdate_joyple_kst >= '{two_weeks_ago}' -- 최근 2주 정도? 
            and osuser = 'And'#And UA User 필터
            and gcat = 'UA' and media_category in ('ADNW','Facebook','Google') #And UA User 필터
            group by regdate_joyple_kst-- , geo_user_group  --- 전체> 국가 group 제외 
            order by 1
            """

            logger.info("🔍 BigQuery 쿼리 실행 중...")
            df_all = bigquery_client.query(query).to_dataframe()
            logger.info(f"✅ 데이터 추출 완료: {len(df_all)} rows")

            # HTML 표 생성 (제공된 형식 참고)
            html_table_header, html_table_rows =format_table(df_all)


            query2 = basic_query + f"""
            select regdate_joyple_kst as Date, geo_user_group as Country
            , CAST(sum(cost_exclude_credit) AS INT64) as Cost
            , ROUND(sum(install), 2) as Install
            , ROUND(sum(ru), 2) as Ru
            --, ROUND(SUM(CASE WHEN gcat = "Organic" or gcat = "Unknown" then ru end) / sum(ru), 2) as Organic_ratio
            , ROUND(sum(cost_exclude_credit)/sum(install), 2) as CPI 
            , ROUND(sum(cost_exclude_credit)/sum(ru), 2)  as CPRU
            , ROUND(sum(rev_d0)/sum(ru), 2)  as D0LTV
            , ROUND(sum(rev_d1)/sum(ru), 2)  as D1LTV
            , ROUND(sum(rev_d3)/sum(ru), 2)  as D3LTV
            , ROUND(sum(rev_d7)/sum(ru), 2)  as D7LTV
            , ROUND(sum(rev_dcum)/sum(ru), 2)  as DcumLTV
            , ROUND(sum(ru_d1)/sum(ru)*100, 2)  as D1RET
            , ROUND(sum(ru_d3)/sum(ru)*100, 2)  as D3RET
            , ROUND(sum(ru_d7)/sum(ru)*100, 2)  as D7RET
            , ROUND(sum(rev_d0)/sum(cost_exclude_credit)*100, 2)  as D0ROAS
            , ROUND(sum(rev_d1)/sum(cost_exclude_credit)*100, 2)  as D1ROAS
            , ROUND(sum(rev_d3)/sum(cost_exclude_credit)*100, 2)  as D3ROAS
            , ROUND(sum(rev_d7)/sum(cost_exclude_credit)*100, 2)  as D7ROAS
            , ROUND(sum(rev_dcum)/sum(cost_exclude_credit)*100, 2)  as DcumROAS      
            from final2 
            where regdate_joyple_kst >= '{two_weeks_ago}' -- 최근 2주 정도? 
            and osuser = 'And'#And UA User 필터
            and gcat = 'UA' and media_category in ('ADNW','Facebook','Google') #And UA User 필터
            group by regdate_joyple_kst, geo_user_group  --- 전체> 국가 group 제외 
            order by 2, 1

            """

            logger.info("🔍 BigQuery 쿼리 실행 중...")
            df_all_geo = bigquery_client.query(query2).to_dataframe()
            logger.info(f"✅ 데이터 추출 완료: {len(df_all_geo)} rows")

            # HTML 표 생성 (제공된 형식 참고)
            df_all_us = df_all_geo[df_all_geo['Country'] == '1.US']
            df_all_jp = df_all_geo[df_all_geo['Country'] == '2.JP']
            df_all_weu = df_all_geo[df_all_geo['Country'] == '3.WEU']
            df_all_etc = df_all_geo[df_all_geo['Country'] == '4.ETC']

            html_table_header_all_us, html_table_rows_all_us = format_table(df_all_us)
            html_table_header_all_jp, html_table_rows_all_jp = format_table(df_all_jp)
            html_table_header_all_weu, html_table_rows_all_weu = format_table(df_all_weu)
            html_table_header_all_etc, html_table_rows_all_etc = format_table(df_all_etc)


            query3 = basic_query + f"""
            select regdate_joyple_kst as Date--, geo_user_group 
            , CAST(sum(cost_exclude_credit) AS INT64) as Cost
            , ROUND(sum(install), 2) as Install
            , ROUND(sum(ru), 2) as Ru
            , CONCAT(CAST(ROUND(SUM(CASE WHEN gcat = "Organic" or gcat = "Unknown" then ru end) / sum(ru) * 100, 2) AS STRING), '%') as Organic_ratio
            , ROUND(sum(cost_exclude_credit)/sum(install), 2) as CPI 
            , ROUND(sum(cost_exclude_credit)/sum(ru), 2)  as CPRU
            , ROUND(sum(rev_d0)/sum(ru), 2)  as D0LTV
            , ROUND(sum(rev_d1)/sum(ru), 2)  as D1LTV
            , ROUND(sum(rev_d3)/sum(ru), 2)  as D3LTV
            , ROUND(sum(rev_d7)/sum(ru), 2)  as D7LTV
            , ROUND(sum(rev_dcum)/sum(ru), 2)  as DcumLTV
            , ROUND(sum(ru_d1)/sum(ru)*100, 2)  as D1RET
            , ROUND(sum(ru_d3)/sum(ru)*100, 2)  as D3RET
            , ROUND(sum(ru_d7)/sum(ru)*100, 2)  as D7RET
            , ROUND(sum(rev_d0)/sum(cost_exclude_credit)*100, 2)  as D0ROAS
            , ROUND(sum(rev_d1)/sum(cost_exclude_credit)*100, 2)  as D1ROAS
            , ROUND(sum(rev_d3)/sum(cost_exclude_credit)*100, 2)  as D3ROAS
            , ROUND(sum(rev_d7)/sum(cost_exclude_credit)*100, 2)  as D7ROAS
            , ROUND(sum(rev_dcum)/sum(cost_exclude_credit)*100, 2)  as DcumROAS   
            from final2 
            where regdate_joyple_kst >= '{two_weeks_ago}' -- 최근 2주 정도? 
            --and osuser = 'And'#And UA User 필터
            --and gcat = 'UA' and media_category in ('ADNW','Facebook','Google') #And UA User 필터
            group by regdate_joyple_kst--, geo_user_group  --- 전체> 국가 group 제외 
            order by 1

            """

            logger.info("🔍 BigQuery 쿼리 실행 중...")
            df_non = bigquery_client.query(query3).to_dataframe()
            logger.info(f"✅ 데이터 추출 완료: {len(df_non)} rows")

            # HTML 표 생성 (제공된 형식 참고)
            html_table_header_non, html_table_rows_non =format_table(df_non)


            query4 = basic_query + f"""
            select regdate_joyple_kst as Date, geo_user_group as Country
            , CAST(sum(cost_exclude_credit) AS INT64) as Cost
            , ROUND(sum(install), 2) as Install
            , ROUND(sum(ru), 2) as Ru
            , CONCAT(CAST(ROUND(SUM(CASE WHEN gcat = "Organic" or gcat = "Unknown" then ru end) / sum(ru) * 100, 2) AS STRING), '%') as Organic_ratio
            , ROUND(sum(cost_exclude_credit)/sum(install), 2) as CPI 
            , ROUND(sum(cost_exclude_credit)/sum(ru), 2)  as CPRU
            , ROUND(sum(rev_d0)/sum(ru), 2)  as D0LTV
            , ROUND(sum(rev_d1)/sum(ru), 2)  as D1LTV
            , ROUND(sum(rev_d3)/sum(ru), 2)  as D3LTV
            , ROUND(sum(rev_d7)/sum(ru), 2)  as D7LTV
            , ROUND(sum(rev_dcum)/sum(ru), 2)  as DcumLTV
            , ROUND(sum(ru_d1)/sum(ru)*100, 2)  as D1RET
            , ROUND(sum(ru_d3)/sum(ru)*100, 2)  as D3RET
            , ROUND(sum(ru_d7)/sum(ru)*100, 2)  as D7RET
            , ROUND(sum(rev_d0)/sum(cost_exclude_credit)*100, 2)  as D0ROAS
            , ROUND(sum(rev_d1)/sum(cost_exclude_credit)*100, 2)  as D1ROAS
            , ROUND(sum(rev_d3)/sum(cost_exclude_credit)*100, 2)  as D3ROAS
            , ROUND(sum(rev_d7)/sum(cost_exclude_credit)*100, 2)  as D7ROAS
            , ROUND(sum(rev_dcum)/sum(cost_exclude_credit)*100, 2)  as DcumROAS   
            from final2 
            where regdate_joyple_kst >= '{two_weeks_ago}' -- 최근 2주 정도? 
            --and osuser = 'And'#And UA User 필터
            --and gcat = 'UA' and media_category in ('ADNW','Facebook','Google') #And UA User 필터
            group by regdate_joyple_kst, geo_user_group  --- 전체> 국가 group 제외 
            order by 2, 1

            """

            logger.info("🔍 BigQuery 쿼리 실행 중...")
            df_non_geo = bigquery_client.query(query4).to_dataframe()
            logger.info(f"✅ 데이터 추출 완료: {len(df_non_geo)} rows")

            # HTML 표 생성 (제공된 형식 참고)
            df_non_us = df_non_geo[df_non_geo['Country'] == '1.US']
            df_non_jp = df_non_geo[df_non_geo['Country'] == '2.JP']
            df_non_weu = df_non_geo[df_non_geo['Country'] == '3.WEU']
            df_non_etc = df_non_geo[df_non_geo['Country'] == '4.ETC']

            html_table_header_non_us, html_table_rows_non_us = format_table(df_non_us)
            html_table_header_non_jp, html_table_rows_non_jp = format_table(df_non_jp)
            html_table_header_non_weu, html_table_rows_non_weu = format_table(df_non_weu)
            html_table_header_non_etc, html_table_rows_non_etc = format_table(df_non_etc)


            # 제미나이 해석 추가
            print("📧 제미나이 해석 추가 진행 중 ...")
            genai_all_us = genai_paid_geo_analytics(df_all_us)
            genai_all_jp = genai_paid_geo_analytics(df_all_jp)
            genai_all_weu = genai_paid_geo_analytics(df_all_weu)
            genai_all_etc = genai_paid_geo_analytics(df_all_etc)
            genai_all = genai_paid_all_analytics(df_all, genai_all_us + genai_all_jp + genai_all_weu + genai_all_etc)
            
            print("📧 Paid 유저에 대한 제미나이 분석 완료")
            genai_non_us = genai_organic_geo_analytics(df_non_us)
            genai_non_jp = genai_organic_geo_analytics(df_non_jp)
            genai_non_weu = genai_organic_geo_analytics(df_non_weu)
            genai_non_etc = genai_organic_geo_analytics(df_non_etc)
            genai_non = genai_organic_all_analytics(df_non, genai_non_us + genai_non_jp + genai_non_weu + genai_non_etc)
            print("📧 Organic 포함 전체 유저에 대한 제미나이 분석 완료")

            print("✅ 제미나이 해석 완료!")

            # 이메일 HTML 본문 생성 (메일 클라이언트 호환성을 위해 인라인 스타일 사용)
            current_time = datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S")
            html_body = f"""<!DOCTYPE html>
                        <html lang="ko">
                        <head>
                            <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
                            <meta http-equiv="Content-Script-Type" content="text/javascript">
                            <meta http-equiv="Content-Style-Type" content="text/css">
                            <meta http-equiv="X-UA-Compatible" content="IE=edge">
                            <meta name="robots" content="noindex, nofollow">
                            <title>Joyple UA Performance & Cost Report</title>
                            <style>
                                body {{
                                    padding: 10px;
                                    margin: 0;
                                    width: 100%;
                                    font-family: Arial, Verdana, Gulim;
                                    font-size: 8pt;
                                }}
                                table {{
                                    width: 100%;
                                    display: table;
                                    border-collapse: collapse;
                                }}
                                tr {{
                                    display: table-row;
                                    vertical-align: inherit;
                                    border-color: inherit;
                                }}
                                tr:nth-child(odd) {{
                                    background: #f2f2f2;
                                    text-align: right;
                                    color: #555555;
                                }}
                                tr:nth-child(even) {{
                                    background: white;
                                    text-align: right;
                                    color: #555555;
                                }}
                                td {{
                                    padding: 3px;
                                    border: 1px #d6d6d6 solid;
                                    text-align: center;
                                    color: black;
                                    white-space: nowrap;
                                }}
                                tr.data1 td {{
                                    background: white;
                                    text-align: right;
                                    color: #555555;
                                }}
                                tr.data2 td {{
                                    background: #f2f2f2;
                                    text-align: right;
                                    color: #555555;
                                }}
                                tr.data-title td {{
                                    background: #eaeaec;
                                    text-align: center;
                                    color: black;
                                    font-weight: bold;
                                    border: 1px #d6d6d6 solid;
                                }}
                                .tableTitleNew1 {{
                                    padding: 5px;
                                    text-align: left;
                                    font-weight: bold;
                                    font-size: 8pt;
                                    background: #707070;
                                    color: white;
                                    border: 1px #2e2e2e solid !important;
                                }}
                                .tableTitleNewMain {{
                                    padding: 5px;
                                    text-align: left;
                                    font-weight: bold;
                                    font-size: 9pt;
                                    background: #424242;
                                    color: white;
                                    border: 1px #2e2e2e solid !important;
                                }}
                                .tableTitleNewgenai {{
                                    padding: 5px;
                                    text-align: left;
                                    font-size: 10pt;
                                    background: #E5E5E5;
                                    color: black;
                                    border: 1px #2e2e2e solid !important;
                                }}
                                .pcenter {{
                                    text-align: center !important;
                                }}
                                .pleft {{
                                    text-align: left !important;
                                }}
                            </style>
                        </head>
                        <body>
                            <table border="1" width="100%">
                                <tbody>
                                    <tr>
                                        <td style="white-space: nowrap" class="tableTitleNewMain">
                                            📊 RESU UA Performance & Cost Report :: {current_time} (KST)
                                        </td>
                                    </tr>
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    <tr>
                                        <td style="white-space:nowrap" class="tableTitleNewMain">전체 유저 조회 기간: {two_weeks_ago} ~ {yesterday} | 총 행 수: {len(df_non)}</td>
                                    </tr>
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    {html_table_header_non}
                                    {html_table_rows_non}
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    <tr>
                                        <td style="white-space:nowrap" class="tableTitleNewgenai">
                                        {genai_non}
                                        </td>
                                    </tr>
                                </tbody>
                            </table>                            

                            <table border="1" width="100%">
                                <tbody>
                                    <tr>
                                        <td style="white-space:nowrap" class="tableTitleNew1">전체 유저(US) 조회 기간: {two_weeks_ago} ~ {yesterday} | 총 행 수: {len(df_non_us)}</td>
                                    </tr>
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    {html_table_header_non_us}
                                    {html_table_rows_non_us}
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    <tr>
                                        <td style="white-space:nowrap" class="tableTitleNewgenai">
                                        {genai_non_us}
                                        </td>
                                    </tr>
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    <tr>
                                        <td style="white-space:nowrap" class="tableTitleNew1">전체 유저(JP) 조회 기간: {two_weeks_ago} ~ {yesterday} | 총 행 수: {len(df_non_jp)}</td>
                                    </tr>
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    {html_table_header_non_jp}
                                    {html_table_rows_non_jp}
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    <tr>
                                        <td style="white-space:nowrap" class="tableTitleNewgenai">
                                        {genai_non_jp}
                                        </td>
                                    </tr>
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    <tr>
                                        <td style="white-space:nowrap" class="tableTitleNew1">전체 유저(WEU) 조회 기간: {two_weeks_ago} ~ {yesterday} | 총 행 수: {len(df_non_weu)}</td>
                                    </tr>
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    {html_table_header_non_weu}
                                    {html_table_rows_non_weu}
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    <tr>
                                        <td style="white-space:nowrap" class="tableTitleNewgenai">
                                        {genai_non_weu}
                                        </td>
                                    </tr>
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    <tr>
                                        <td style="white-space:nowrap" class="tableTitleNew1">전체 유저(ETC) 조회 기간: {two_weeks_ago} ~ {yesterday} | 총 행 수: {len(df_non_etc)}</td>
                                    </tr>
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    {html_table_header_non_etc}
                                    {html_table_rows_non_etc}
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    <tr>
                                        <td style="white-space:nowrap" class="tableTitleNewgenai">
                                        {genai_non_etc}
                                        </td>
                                    </tr>
                                </tbody>
                            </table>
                            
                            <br>

                            <table border="1" width="100%">
                                <tbody>
                                    <tr>
                                        <td style="white-space:nowrap" class="tableTitleNewMain">Android Paid User 조회 기간: {two_weeks_ago} ~ {yesterday} | 총 행 수: {len(df_all)}</td>
                                    </tr>
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    {html_table_header}
                                    {html_table_rows}
                                </tbody>
                            </table>
                            
                            <table border="1" width="100%">
                                <tbody>
                                    <tr>
                                        <td style="white-space:nowrap" class="tableTitleNewgenai">
                                        {genai_all}
                                        </td>
                                    </tr>
                                </tbody>
                            </table>    

                            <table border="1" width="100%">
                                <tbody>
                                    <tr>
                                        <td style="white-space:nowrap" class="tableTitleNew1">Android Paid User(US) 조회 기간: {two_weeks_ago} ~ {yesterday} | 총 행 수: {len(df_all_us)}</td>
                                    </tr>
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    {html_table_header_all_us}
                                    {html_table_rows_all_us}
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    <tr>
                                        <td style="white-space:nowrap" class="tableTitleNewgenai">
                                        {genai_all_us}
                                        </td>
                                    </tr>
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    <tr>
                                        <td style="white-space:nowrap" class="tableTitleNew1">Android Paid User(JP) 조회 기간: {two_weeks_ago} ~ {yesterday} | 총 행 수: {len(df_all_jp)}</td>
                                    </tr>
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    {html_table_header_all_jp}
                                    {html_table_rows_all_jp}
                                </tbody>    
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    <tr>
                                        <td style="white-space:nowrap" class="tableTitleNewgenai">
                                        {genai_all_jp}
                                        </td>
                                    </tr>
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    <tr>
                                        <td style="white-space:nowrap" class="tableTitleNew1">Android Paid User(WEU) 조회 기간: {two_weeks_ago} ~ {yesterday} | 총 행 수: {len(df_all_weu)}</td>
                                    </tr>
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    {html_table_header_all_weu}
                                    {html_table_rows_all_weu}
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    <tr>
                                        <td style="white-space:nowrap" class="tableTitleNewgenai">
                                        {genai_all_weu}
                                        </td>
                                    </tr>
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    <tr>
                                        <td style="white-space:nowrap" class="tableTitleNew1">Android Paid User(ETC) 조회 기간: {two_weeks_ago} ~ {yesterday} | 총 행 수: {len(df_all_etc)}</td>
                                    </tr>
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    {html_table_header_all_etc}
                                    {html_table_rows_all_etc}
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    <tr>
                                        <td style="white-space:nowrap" class="tableTitleNewgenai">
                                        {genai_all_etc}
                                        </td>
                                    </tr>
                                </tbody>
                            </table>

                            <div style="text-align: center; margin-top: 20px; padding-top: 10px; border-top: 1px solid #ddd; color: #999; font-size: 8pt;">
                                <p>자동 생성된 이메일입니다. 회신하지 마세요.</p>
                            </div>
                        </body>
                        </html>
                        """

            # 이메일 발송
            logger.info("📧 이메일 발송 중...")

            server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=10)
            server.set_debuglevel(0)  # 디버그 모드 끄기
            
            # # 인증이 필요하면
            # if SENDER_PASSWORD:
            #     server.login(SENDER_EMAIL, SENDER_PASSWORD)
            
            msg = MIMEMultipart()
            msg['From'] = SENDER_EMAIL
            msg['To'] = ', '.join(RECIPIENT_EMAILS)
            msg['Subject'] = f"[RESU] UA Performance & Cost Report {today}"
            msg.attach(MIMEText(html_body, 'html'))
            
            server.sendmail(SENDER_EMAIL, RECIPIENT_EMAILS, msg.as_string())
            server.quit()
            print("메일 발송 성공")

            # msg = MIMEMultipart()
            # msg['From'] = SENDER_EMAIL
            # msg['To'] = ', '.join(RECIPIENT_EMAILS)
            # msg['Subject'] = f"[RESU] UA Performance & Cost Report {today}"
            # msg.attach(MIMEText(html_body, 'html'))

            # with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            #     server.starttls()
            #     # server.login(SENDER_EMAIL, SENDER_PASSWORD)
            #     server.sendmail(SENDER_EMAIL, RECIPIENT_EMAILS, msg.as_string())

            # logger.info(f"✅ 이메일 발송 완료: {RECIPIENT_EMAILS}")
            # return True

        except Exception as e:
            logger.error(f"❌ 에러 발생: {str(e)}", exc_info=True)
            raise

    # Task 정의
    task = PythonOperator(
        task_id='extract_and_send_email',
        python_callable=extract_and_send_email,
        dag=dag,
    )