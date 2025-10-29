from datetime import datetime, timedelta
import json
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
    today = datetime.today().strftime('%Y-%m-%d')
    two_days_ago = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    
    failed_projects = []
    total_rows = 0
    uploaded_files = []
    
    for project_name in project_list:
        if not project_name.strip():
            continue
        try:
            query = """
                SELECT *
                FROM `datacatalog-446301.UA_Service.ru_mailing_data_test`
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
            df.to_csv(csv_buffer, index=False, encoding='utf-8')
            blob_name = f"all_reports/{project_name}_{today}.csv"
            blob = bucket.blob(blob_name)
            blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")
            print(f"✓ GCS 업로드 완료: {blob.name}")
            
            expiration = timedelta(hours=24)
            signed_url = blob.generate_signed_url(version="v4", expiration=expiration, method="GET")
            cell = worksheet.find(project_name)
            if cell:
                worksheet.update_cell(cell.row, cell.col + 1, signed_url)
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
    print("=" * 80)
    print("Task 2: 대행사 보고서 생성 시작")
    print("=" * 80)
    
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
        RU_D3, RU_D7, RU_D14, RU_D30, RU_D60, RU_D90
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
                agency_filter_condition = "AND media_category = 'ADNW' AND agency IN ('dotlab','NULL', 'webmediail626')"
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
                FROM `datacatalog-446301.UA_Service.ru_mailing_data_test`
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
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, encoding='utf-8')
            
            # GCS에 업로드
            blob_name = f"agency_reports/{pr_name}_{agency_name}_{today}.csv"
            blob = bucket.blob(blob_name)
            blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")

            print(f"✓ GCS 업로드 완료: {blob.name}")
            
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


# ============= Task 3: 이메일 발송 =============
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
# Task 정의
task_send_email = PythonOperator(
    task_id='send_status_email',
    python_callable=send_status_email,
    trigger_rule='all_done',  # 이전 task 성공/실패 관계없이 항상 실행
    dag=dag,
)

# Task 의존성
task_all_projects >> task_agency_reports >> task_send_email