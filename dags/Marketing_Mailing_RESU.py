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
    dag_id='Marketing_Mailing_RESU',
    default_args=default_args,
    description='RESU 마케팅 결과를 메일링',
    schedule='30 20 * * *',
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
    SMTP_SERVER = get_var('SMTP_SERVER', 'smtp.gmail.com')
    SMTP_PORT = int(get_var('SMTP_PORT', '587'))
    SENDER_EMAIL = get_var('EMAIL_FROM')
    SENDER_PASSWORD = get_var('SMTP_PASSWORD')

    # 수신자 설정
    RECIPIENT_EMAILS = 'seongin@joycity.com'
    # RECIPIENT_EMAILS = [email.strip() for email in get_var('RECIPIENT_EMAILS', '').split(',') if email.strip()]

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
    two_weeks_ago = today - timedelta(days=14) # 2주 전 데이터 기준으로 가져오기

    def extract_and_send_email(**context):
        """쿼리 실행 및 이메일 발송"""
        try:
            # BigQuery 쿼리 실행
            query = f"""
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
            , case when CountryCode = 'KR' then '1.KR'
                when CountryCode = 'US' then '2.US'
                when CountryCode = 'JP' then '3.JP'
                when CountryCode in ('UK','FR','DE','GB') then '4.WEU'
                else '5.ETC' end as geo_user_group 
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
            from (select  * , case when CountryCode = 'KR' then '1.KR'
                when CountryCode = 'US' then '2.US'
                when CountryCode = 'JP' then '3.JP'
                when CountryCode in ('UK','FR','DE','GB') then '4.WEU'
                else '5.ETC' end as geo_user_group 
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



            
            select regdate_joyple_kst --, geo_user_group 
            , ROUND(sum(cost_exclude_credit),0) as cost
            , ROUND(sum(install), 0) as install
            , ROUND(sum(ru), 0) as ru
            , ROUND(sum(cost_exclude_credit)/sum(install), 1) as CPI 
            , ROUND(sum(cost_exclude_credit)/sum(ru), 0)  as CPRU
            , ROUND(sum(rev_d0)/sum(ru), 0)  as D0LTV
            , ROUND(sum(rev_d1)/sum(ru), 0)  as D1LTV
            , ROUND(sum(rev_d3)/sum(ru), 0)  as D3LTV
            , ROUND(sum(rev_d7)/sum(ru), 0)  as D7LTV
            , ROUND(sum(rev_dcum)/sum(ru), 0)  as DcumLTV
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
            html_table_header = '<tr class="data-title">'
            for col in df_all.columns:
                html_table_header += f'<td>{col}</td>'
            html_table_header += '</tr>'

            html_table_rows = ''
            for idx, row in df_all.iterrows():
                row_class = 'data1' if idx % 2 == 0 else 'data2'
                html_table_rows += f'<tr class="{row_class}">'
                for cell in row:
                    cell_value = '' if pd.isna(cell) else str(cell)
                    html_table_rows += f'<td>{cell_value}</td>'
                html_table_rows += '</tr>'


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
                                    background: #424242;
                                    color: white;
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
                                        <td style="white-space: nowrap" class="tableTitleNew1">
                                            📊 Joyple UA Performance & Cost Report :: {current_time} (KST)
                                        </td>
                                    </tr>
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    <tr>
                                        <td style="white-space:nowrap" class="tableTitleNew1">조회 기간: {two_weeks_ago} ~ {today} | 총 행 수: {len(df_all)}</td>
                                    </tr>
                                </tbody>
                            </table>

                            <table border="1" width="100%">
                                <tbody>
                                    {html_table_header}
                                    {html_table_rows}
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
            msg = MIMEMultipart()
            msg['From'] = SENDER_EMAIL
            msg['To'] = ', '.join(RECIPIENT_EMAILS)
            msg['Subject'] = "[Joyple] UA Performance & Cost Report"
            
            # 이메일 헤더 추가 (스팸 필터 회피)
            import email.utils
            msg['Message-ID'] = email.utils.make_msgid()
            msg['Date'] = email.utils.formatdate(localtime=True)
            msg['Reply-To'] = SENDER_EMAIL
            msg['X-Mailer'] = 'Joyple-Airflow/1.0'
            
            # 텍스트 버전 추가 (Plain Text - HTML이 로드 안 될 경우 대체)
            text_body = f"""Joyple UA Performance & Cost Report
            조회 기간: {two_weeks_ago} ~ {today}
            총 행 수: {len(df_all)}
            생성 시간: {datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S")} (KST)

            상세 내용은 HTML 형식의 이메일을 참고하세요.

            자동 생성된 이메일입니다. 회신하지 마세요.
            """
            msg.attach(MIMEText(text_body, 'plain', 'utf-8'))
            
            # HTML 버전 추가
            msg.attach(MIMEText(html_body, 'html', 'utf-8'))



            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(SENDER_EMAIL, SENDER_PASSWORD)
                server.sendmail(SENDER_EMAIL, RECIPIENT_EMAILS, msg.as_string())

            logger.info(f"✅ 이메일 발송 완료: {RECIPIENT_EMAILS}")
            return True

        except Exception as e:
            logger.error(f"❌ 에러 발생: {str(e)}", exc_info=True)
            raise

    # Task 정의
    task = PythonOperator(
        task_id='extract_and_send_email',
        python_callable=extract_and_send_email,
        dag=dag,
    )