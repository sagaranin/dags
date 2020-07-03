from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
from operators.postgres_to_clickhouse import PostgresToClickhouseOperator

default_args = {
    'owner': 'airflow',
    'wait_for_downstream': False,
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1, 0, 0, 0),
    'email': ['sa.garanin@gmail.com'],
    'email_on_success': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG('WIKI_UPD_Fact', default_args=default_args, schedule_interval='@daily') as dag:
    
    load_eventlog_facts = PostgresToClickhouseOperator(
        task_id='load_eventlog_facts',
        postgres_conn_id='postgres_db_wiki',
        clickhouse_conn_id='clickhouse_db_default',
        batch_size=1000,
        select_query="select * from events_current where meta_dt >= '{{ prev_ds }}'::date and meta_dt < '{{ ds }}'::date ",
        insert_query='INSERT INTO events_fact (meta_id,meta_dt,meta_request_id,meta_partition,meta_offset,uri,id,bot,`type`,namespace,`user`,title,comment,server_name,wiki,length_old,length_new,revision_old,revision_new) VALUES '
    )

    load_eventlog_facts 
