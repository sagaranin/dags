from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'wait_for_downstream': True,
    'depends_on_past': True,
    'start_date': datetime(2020, 4, 1),
    'email': ['sa.garanin@gmail.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

with DAG('WIKI_UPD_Fact', default_args=default_args, schedule_interval='@hourly', catchup=False) as dag:

    update_eventlog_facts = PostgresOperator(
        task_id='update_eventlog_facts',
        sql= """
            insert into events_fact 
            (select * from events e 
                where meta_dt >= (select coalesce(max(meta_dt), '1970-01-01'::date) from events_fact)  
                order by meta_dt limit {{ var.value.wiki_upd_batch_size }})
            on conflict do nothing;
        """,
        postgres_conn_id='postgres_db_wiki',
        autocommit=True
    )

    update_eventlog_facts 
