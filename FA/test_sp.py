from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from operators.oracle_to_postgres import OracleToPostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.oracle_hook import OracleHook
from airflow.models import Variable
from datetime import datetime, timedelta
from psycopg2.extras import DictCursor, execute_values
import cx_Oracle
from contextlib import closing

import logging


default_args = {
    'owner': 'airflow',
    'wait_for_downstream': True,
    'depends_on_past': True,
    'start_date': datetime(2020, 4, 1, 0, 0, 0),
    'retries': 0
}


######################################
###    DAG and Tasks definition    ###
######################################
with DAG('test', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    def get_data(ds, **kwargs):
        with closing(OracleHook(oracle_conn_id='oracle_src').get_conn()) as src_conn:
            src_cursor = src_conn.cursor()
            refCursor1 = src_cursor.callfunc('SYSTEM.GET_DATA1', cx_Oracle.CURSOR)

            for row1 in refCursor1:
                print(row1)

    test = PythonOperator(
        task_id='test',
        provide_context=True,
        python_callable=get_data
    )

    test

