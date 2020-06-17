from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta

import pandas, logging

default_args = {
    'owner': 'airflow',
    'wait_for_downstream': False,
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'end_date': datetime(2019, 12, 31),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'sla': timedelta(minutes=30),
    'retry_delay': timedelta(minutes=5)
}

with DAG('Postgres2Postgres', default_args=default_args, schedule_interval='@daily') as dag:

    def transfer_daily_data(ds, **kwargs):

        src_engine = PostgresHook(postgres_conn_id='postgres_local').get_sqlalchemy_engine()
        tgt_engine = PostgresHook(postgres_conn_id='postgres_local').get_sqlalchemy_engine()

        execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
        next_date = (kwargs['execution_date']+timedelta(days=1)).strftime('%Y-%m-%d')
        execution_date_u = kwargs['execution_date'].strftime('%Y%m%d')
        logging.info(kwargs)

        data = pandas.read_sql(f"select * from source.history where event_date between date '{execution_date}' and date '{next_date}'", src_engine)
        data.to_sql('history_stg_'+execution_date_u, con=tgt_engine, index=False, if_exists='replace', schema='target')

    move_daily_data = PythonOperator(
        task_id='move_daily_data_to_stg',
        provide_context=True,
        python_callable=transfer_daily_data
    )

    update_target_table = PostgresOperator(
        task_id='update_target_table',
        sql= '''
            insert into target.history
                select * from target.history_stg_{{ ds_nodash }}
            on conflict do nothing;
        ''',
        postgres_conn_id='postgres_local',
        autocommit=True
    )

    drop_stg_table = PostgresOperator(
        task_id='drop_stg_table',
        sql= "drop table target.history_stg_{{ ds_nodash }}",
        postgres_conn_id='postgres_local',
        autocommit=True
    )

    move_daily_data >> update_target_table >> drop_stg_table