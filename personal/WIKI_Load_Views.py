from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.clickhouse_hook import ClickHouseHook
from datetime import datetime, timedelta

import csv, sys
csv.field_size_limit(sys.maxsize)

default_args = {
    'owner': 'airflow',
    'wait_for_downstream': False,
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1, 3, 0, 0),
    'email': ['sa.garanin@gmail.com'],
    'email_on_success': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 12,
    'retry_delay': timedelta(hours=1)
}

def process_csv(ds, **kwargs):
    execution_dt = kwargs['execution_date'].strftime('%Y-%m-%d %H:00:00')
    file_path = f"/tmp/pageviews-{kwargs['execution_date'].strftime('%Y%m%d-%H')}0000"
    tgt_client =  ClickHouseHook(clickhouse_conn_id='clickhouse_db_default').get_conn()
    
    batch = []
    insert_query = "insert into views values"

    with open(file_path, newline='') as csvfile:
        reader = csv.reader((line.replace('\0','') for line in csvfile), delimiter=' ')
        for row in reader:
            if len(row) == 4 and row[0] in ['en', 'ru']:
                full_row = (datetime.strptime(execution_dt, '%Y-%m-%d %H:%M:%S'), row[0], row[1], int(row[2]))
                batch.append(full_row)

        print(f"Start writing batch {len(batch)}")
        tgt_client.execute(insert_query, batch)
        print("End writing batch")


with DAG('WIKI_Load_Views', default_args=default_args, schedule_interval='@hourly', concurrency=3) as dag:
    
    load_archive = BashOperator(
        task_id='load_archive',
        bash_command='curl -o /tmp/pageviews-{{ execution_date.strftime("%Y%m%d-%H") }}0000.gz --fail https://dumps.wikimedia.org/other/pageviews/{{ execution_date.strftime("%Y") }}/{{ execution_date.strftime("%Y-%m") }}/pageviews-{{ execution_date.strftime("%Y%m%d-%H") }}0000.gz'
    )

    extract_archive = BashOperator(
        task_id='extract_archive',
        bash_command='gzip -df /tmp/pageviews-{{ execution_date.strftime("%Y%m%d-%H") }}0000.gz'
    )

    load_views = PythonOperator(
        task_id='load_views',
        provide_context=True,
        python_callable=process_csv
    )

    drop_file = BashOperator(
        task_id='drop_file',
        bash_command='rm -f /tmp/pageviews-{{ execution_date.strftime("%Y%m%d-%H") }}0000'
    )

    load_archive >> extract_archive >> load_views >> drop_file
