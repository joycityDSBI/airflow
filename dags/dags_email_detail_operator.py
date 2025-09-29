from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from datetime import datetime
from airflow.utils import timezone

def send_simple_summary(**context):
    """간단한 DAG 실행 결과"""
    dag_run = context['dag_run']
    dag_id = context['dag'].dag_id
    ti = context['ti']
    
    # 현재 Task의 시작/종료 시간
    start_date = dag_run.start_date
    end_date = timezone.utcnow()
    duration = end_date - start_date
    
    # DAG 객체에서 Task 목록 가져오기
    dag = context['dag']
    all_tasks = dag.tasks
    
    task_rows = ''
    for task in all_tasks:
        task_rows += f'''
            <tr>
                <td>{task.task_id}</td>
                <td>완료 대기</td>
                <td>-</td>
            </tr>
        '''
    
    html_content = f'''
        <div style="font-family: Arial;">
            <h2 style="color: #4CAF50;">✅ DAG 실행 완료</h2>
            
            <h3>전체 실행 정보</h3>
            <table border="1" cellpadding="8" style="border-collapse: collapse;">
                <tr>
                    <td><b>DAG ID</b></td>
                    <td>{dag_id}</td>
                </tr>
                <tr>
                    <td><b>실행 시작</b></td>
                    <td>{start_date}</td>
                </tr>
                <tr>
                    <td><b>리포트 생성</b></td>
                    <td>{end_date}</td>
                </tr>
                <tr style="background-color: #e8f5e9;">
                    <td><b>경과 시간</b></td>
                    <td>{duration}</td>
                </tr>
                <tr>
                    <td><b>총 Task 수</b></td>
                    <td>{len(all_tasks)}개</td>
                </tr>
            </table>
            
            <h3>✅ 모든 작업이 성공적으로 완료되었습니다!</h3>
        </div>
    '''
    
    EmailOperator(
        task_id='dags_email_detail_operator',
        to='65e43b85.joycity.com@kr.teams.ms',
        subject=f'✅ DAG 완료: {dag_id}',
        html_content=html_content,
        conn_id='conn_smtp_gmail'
    ).execute(context=context)

with DAG(
    dag_id='dag_simple_notification',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    
    task1 = BashOperator(
        task_id='extract_data',
        bash_command='sleep 5 && echo "데이터 추출 완료"'
    )
    
    task2 = BashOperator(
        task_id='transform_data',
        bash_command='sleep 3 && echo "데이터 변환 완료"'
    )
    
    task3 = BashOperator(
        task_id='load_data',
        bash_command='sleep 2 && echo "데이터 적재 완료"'
    )
    
    send_summary = PythonOperator(
        task_id='send_summary',
        python_callable=send_simple_summary
    )
    
    task1 >> task2 >> task3 >> send_summary