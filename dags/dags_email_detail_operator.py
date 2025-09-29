from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from datetime import datetime

def send_dag_summary(**context):
    """DAG 전체 실행 결과 요약"""
    dag_run = context['dag_run']
    dag_id = context['dag'].dag_id
    
    # DAG 실행 시간
    start_date = dag_run.start_date
    end_date = datetime.now()
    duration = end_date - start_date
    
    # 모든 Task 정보 수집
    task_instances = dag_run.get_task_instances()
    
    task_rows = ''
    for ti in task_instances:
        if ti.end_date and ti.start_date:
            task_duration = ti.end_date - ti.start_date
            task_rows += f'''
                <tr>
                    <td>{ti.task_id}</td>
                    <td>{ti.state}</td>
                    <td>{task_duration}</td>
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
                    <td><b>시작 시간</b></td>
                    <td>{start_date}</td>
                </tr>
                <tr>
                    <td><b>종료 시간</b></td>
                    <td>{end_date}</td>
                </tr>
                <tr style="background-color: #e8f5e9;">
                    <td><b>총 소요 시간</b></td>
                    <td>{duration}</td>
                </tr>
            </table>
            
            <h3>Task별 실행 시간</h3>
            <table border="1" cellpadding="8" style="border-collapse: collapse;">
                <tr style="background-color: #f0f0f0;">
                    <th>Task</th>
                    <th>상태</th>
                    <th>소요 시간</th>
                </tr>
                {task_rows}
            </table>
        </div>
    '''
    
    EmailOperator(
        task_id='summary_email',
        to='65e43b85.joycity.com@kr.teams.ms',
        subject=f'✅ DAG 완료: {dag_id} (소요시간: {duration})',
        html_content=html_content,
        conn_id='conn_smtp_gmail'
    ).execute(context=context)

with DAG(
    dag_id='dag_summary_notification',
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
        python_callable=send_dag_summary
    )
    
    task1 >> task2 >> task3 >> send_summary