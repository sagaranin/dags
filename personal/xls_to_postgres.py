from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago

import pandas, logging

lfp = "/home/garanin/tmpfiles/mining_12_po.xlsx"
rfp = "/opt/data/beeline/data2/mining_12_po.xlsx"

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'wait_for_downstream': True
}

with DAG('Beeline_PO2DB', default_args=default_args, description='Тестовый поток. XLS -> PostgreSQL', schedule_interval=timedelta(hours=6)) as dag:

    dag.doc_md = __doc__

    # Получаем файл по SFTP
    fetch_file = SFTPOperator(
        task_id="sftp_get_file",
        ssh_conn_id="ssh_local",
        remote_filepath=rfp,
        local_filepath=lfp,
        operation="get",
        create_intermediate_dirs=True
    )

    def process_xls_file(ds, **kwargs):
        file = pandas.read_excel(Path(lfp))
        file.columns = file.columns.map(lambda x: x.replace('(', '').replace(')', ''))   # удаляем символы скобок из имен колонок
        engine = PostgresHook(postgres_conn_id='postgres_local').get_sqlalchemy_engine()
        file.to_sql('airflow_stg_mining_po', con=engine, index=True, if_exists='replace', schema='beeline')

    # читаем файл и записываем во временную таблицу целевой БД 
    process_file = PythonOperator(
        task_id='process_file',
        provide_context=True,
        python_callable=process_xls_file
    )

    process_file.doc_md = """\
        #### Task Documentation
        You can document your task using the attributes `doc_md` (markdown),
        `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
        rendered in the UI's Task Instance Details page.
        ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
        """

    # обновляем целевую таблицу 
    update_target_table = PostgresOperator(
        task_id='update_target_table',
        sql= '''
            insert into beeline.airflow_mining_po 
                select * from beeline.airflow_stg_mining_po
            on conflict do nothing;
        ''',
        postgres_conn_id='postgres_local',
        autocommit=True
    )

    fetch_file >> process_file >> update_target_table