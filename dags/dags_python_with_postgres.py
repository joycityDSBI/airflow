import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id = 'dags_python_with_postgres',
    description = 'A simple DAG to test PythonOperator with Postgres',
    schedule = None,
    start_date = datetime(2025, 9, 15),
    catchup = False,
) as dag:


    def insrt_postgres(ip, port, dbname, user, passwd, **kwargs):
        import psycopg2
        from contextlib import closing
        with closing(psycopg2.connect(
            host=ip,
            port=int(port),
            dbname=dbname,
            user=user,
            password=passwd
        )) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'insert 수행'
                sql = 'insert into py_opt_drct_insrt values (%s, %s, %s, %s);'
                cursor.execute(sql, (dag_id, task_id, run_id, msg))
                conn.commit()

    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs=['172.28.0.3', '5432', 'airflow', 'airflow', 'airflow']
    )

    insrt_postgres