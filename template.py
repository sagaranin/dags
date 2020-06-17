from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
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
    'retries': 0,
    # 'retry_delay': timedelta(minutes=5)
}


def load_data(ds, **kwargs):
    """ Выполняет чтение данных из таблицы источника и запись в целевую БД. 
        Параметры (таблицы, поля, условия) передаются через kwargs
    """
    batch_size = int(Variable.get("oebs.select.batch.size", default_var=5000))

    select_query = "select {fields} from {src_schema}.{src_table}".format(**kwargs)
    if kwargs['use_conditions']:
        select_query += " where {conditions}".format(**kwargs)
    logging.info(f"Run query: \"{select_query}\"")

    src_conn = OracleHook(oracle_conn_id='oracle_src').get_conn()
    src_cursor = src_conn.cursor("serverCursor")
    src_cursor.execute(select_query)

    tgt_cursor = tgt_conn.cursor()
    tgt_cursor.execute("truncate table {tgt_schema}.stg_{tgt_table}".format(**kwargs))
    

    batch_count = 0
    while True:
        logging.info(f"Processing batch:\t{batch_count}, size: {batch_size}")
        records = src_cursor.fetchmany(batch_size)
        
        if not records:
            logging.info("Передача данных из таблицы {src_table} завершена".format(**kwargs))
            break
        else:
            execute_values(  # вставка батча данных
                tgt_cursor,
                "INSERT INTO {tgt_schema}.stg_{tgt_table} ({fields}) VALUES %s".format(**kwargs),
                records
            )

        batch_count += 1
    
    tgt_conn.commit()
    src_cursor.close()
    tgt_cursor.close()

def generate_query():
    """Выполняет генерацию запроса для обновления основных таблиц из stage-таблиц"""
    with tgt_conn.cursor(cursor_factory=DictCursor) as cursor:
        query = "BEGIN TRANSACTION;\n"

        cursor.execute(metadata_query)
        for row in cursor:

            if row['load_type'] == 'f':  # full
                query += "truncate table {tgt_table};\n".format(**row)
                query += "insert into {tgt_table} ({fields}) select {fields} from stg_{tgt_table};\n\n".format(**row)

            elif row['load_type'] == 'i':  # increment
                query += "delete from {tgt_table} where {key_field} in (select {key_field} from stg_{tgt_table});\n".format(**row)
                query += "insert into {tgt_table} ({fields}) select {fields} from stg_{tgt_table};\n\n".format(**row)
        
        query += "COMMIT; \n"
        logging.info(f"Сгенерирован запрос: \n {query}")
        return query

######################################
###    DAG and Tasks definition    ###
######################################
with DAG('OEBS_Data_Load', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    metadata_query = 'SELECT src_schema, src_table, fields, key_field, tgt_schema, tgt_table, load_type, conditions, use_conditions FROM public.tables_md'
    tgt_conn = PostgresHook(postgres_conn_id='postgres_tgt').get_conn()


    update_tables = PostgresOperator(
        task_id='update_tables',
        sql=generate_query(),
        postgres_conn_id='postgres_tgt',
        autocommit=False
    )

    # Подключаемся к таблице метаданных, получаем список строк - таблиц источника и 
    #   генерируем подзадачи загрузки данных из источника
    with tgt_conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(metadata_query)
            for row in cursor:
                po = PythonOperator(
                    task_id='load_stg_{tgt_table}'.format(**row),
                    provide_context=True,
                    python_callable=load_data,
                    op_kwargs=dict(row)
                )

                po >> update_tables

    update_activity_table = PostgresOperator(
        task_id='update_activity',
        sql= '''
            select 'bla bla bla';
        ''',
        postgres_conn_id='postgres_tgt',
        autocommit=True
    )

    update_cases_table = PostgresOperator(
        task_id='update_cases',
        sql= '''
            select 'bla bla bla';
        ''',
        postgres_conn_id='postgres_tgt',
        autocommit=True
    )

    update_tables >> [update_activity_table, update_cases_table]

