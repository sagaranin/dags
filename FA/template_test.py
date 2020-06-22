from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators.oracle_to_postgres import OracleToPostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.oracle_hook import OracleHook
from airflow.models import Variable
from datetime import datetime, timedelta
from psycopg2.extras import DictCursor, execute_values

import logging


default_args = {
    'owner': 'airflow',
    'wait_for_downstream': True,
    'depends_on_past': True,
    'start_date': datetime(2020, 4, 1, 0, 0, 0),
    'retries': 0
}


def generate_query():
    """Выполняет генерацию запроса для обновления основных таблиц из stage-таблиц"""
    with tgt_conn.cursor(cursor_factory=DictCursor) as cursor:
        query = ""

        cursor.execute(metadata_query)
        for row in cursor:

            if row['load_type'] == 'f':  # full
                query += "-- '{tgt_table}' full reload\n".format(**row)
                query += "truncate table {tgt_table};\n".format(**row)
                query += "insert into {tgt_table} ({fields}) select {fields} from stg_{tgt_table};\n\n".format(**row)

            elif row['load_type'] == 'i':  # increment
                query += "-- '{tgt_table}' increment load\n".format(**row)
                query += "delete from {tgt_table} where {key_field} in (select {key_field} from stg_{tgt_table});\n".format(**row)
                query += "insert into {tgt_table} ({fields}) select {fields} from stg_{tgt_table};\n\n".format(**row)
        
        logging.info(f"Сгенерирован запрос: \n {query}")
        return query

######################################
###    DAG and Tasks definition    ###
######################################
with DAG('OEBS_Data_Load', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    metadata_query = 'SELECT src_schema, src_table, fields, key_field, tgt_schema, tgt_table, load_type, conditions, use_conditions FROM public.tables_md'
    tgt_conn = PostgresHook(postgres_conn_id='postgres_tgt').get_conn()

    # Подключаемся к таблице метаданных, получаем список строк - таблиц источника и 
    #   генерируем подзадачи загрузки данных из источника
    with tgt_conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(metadata_query)
            for row in cursor:
                params = dict(row)
                params['target_table_prefix'] = 'stg_'

                po = OracleToPostgresOperator(
                    task_id='load_stg_{tgt_table}'.format(**row),
                    oracle_conn_id='oracle_src',
                    postgres_conn_id='postgres_tgt',
                    # provide_context=True,
                    params=params,
                    batch_size=int(Variable.get("oebs.select.batch.size", default_var=5000))
                )

                po 


    tgt_conn.close()