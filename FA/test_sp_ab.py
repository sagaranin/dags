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
with DAG('test2', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    def get_data(ds, **kwargs):
        with closing(OracleHook(oracle_conn_id='oracle_src').get_conn()) as src_conn:
            src_cursor = src_conn.cursor()
            src_cursor.execute(
                """
                DECLARE  
                    l_cursor1 SYS_REFCURSOR;
                    l_cursor2 SYS_REFCURSOR;
                BEGIN
                    OPEN l_cursor1 FOR
                        SELECT 1 as id, 'one' as name from dual union all
                        SELECT 2 as id, 'two' as name from dual;
                    OPEN l_cursor2 FOR
                        SELECT 3 as id, 'tree' as name from dual union all
                        SELECT 4 as id, 'four' as name from dual;
                    DBMS_SQL.RETURN_RESULT(l_cursor1);
                    DBMS_SQL.RETURN_RESULT(l_cursor2);
                END;
                """
            )

            for implicitCursor in src_cursor.getimplicitresults():
                for row in implicitCursor:
                    print(row)

    test = PythonOperator(
        task_id='test2',
        provide_context=True,
        python_callable=get_data
    )

    test

