from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'wait_for_downstream': True,
    'depends_on_past': True,
    'start_date': datetime(2020, 4, 1),
    'email': ['sa.garanin@gmail.com'],
    'email_on_success': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

with DAG('VK_Update_Graph', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    update_eventlog_facts = PostgresOperator(
        task_id='update_edges',
        sql= """
            REFRESH MATERIALIZED VIEW public.edges_mv WITH DATA;
        """,
        postgres_conn_id='postgres_db_vk',
        autocommit=True
    )

    update_friends_graph = PostgresOperator(
        task_id='update_friends_graph',
        sql= """
            REFRESH MATERIALIZED VIEW public.friends_bidirectional_mv WITH DATA;
        """,
        postgres_conn_id='postgres_db_vk',
        autocommit=True
    )

    update_eventlog_facts >> update_friends_graph
