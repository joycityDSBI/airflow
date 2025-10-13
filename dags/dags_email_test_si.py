from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_email_func():
    smtp_server = "smtp.office365.com"
    port = 587
    sender_email = "seongin@joycity.com"
    password = "woeyenikino6911"
    receiver_email = "seongin@joycity.com"
    
    message = MIMEMultipart("alternative")
    message["Subject"] = "Airflow 테스트 이메일"
    message["From"] = sender_email
    message["To"] = receiver_email
    
    html = "<h3>테스트 성공!</h3><p>이메일 발송 완료</p>"
    part = MIMEText(html, "html")
    message.attach(part)
    
    try:
        server = smtplib.SMTP(smtp_server, port)
        server.starttls()  # TLS 암호화
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())
        server.quit()
        print("✅ 이메일 발송 성공!")
    except Exception as e:
        print(f"❌ 이메일 발송 실패: {e}")
        raise

with DAG(
    dag_id='dags_email_test_si',
    start_date=datetime(2025, 10, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    send_email_task = PythonOperator(
        task_id='send_email_task',
        python_callable=send_email_func
    )