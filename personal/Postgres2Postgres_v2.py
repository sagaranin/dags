from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

from psycopg2.extras import execute_values
from datetime import datetime, timedelta

import pandas, logging

default_args = {
    'owner': 'airflow',
    'wait_for_downstream': True,
    'depends_on_past': True,
    'start_date': datetime(2020, 2, 28),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

src_conn = PostgresHook(postgres_conn_id='postgres_local').get_conn()
tgt_conn = PostgresHook(postgres_conn_id='postgres_local').get_conn()

def update_history(ds, **kwargs):

    tgt_cursor = tgt_conn.cursor()
    tgt_cursor.execute("select to_char(coalesce(max(event_date), date '2019-01-01'), 'YYYY-MM-DD HH24:MI:SS') from target.history")
    last_dt = tgt_cursor.fetchone()[0]
    logging.info(f"Последний таймстамп в целевой таблице: '{last_dt}'")

    src_cursor = src_conn.cursor("serverCursor")
    logging.info(f"Выполняется запрос: select * from source.history where event_date >= date '{last_dt}' order by event_date")
    src_cursor.execute(f"select * from source.history where event_date >= date '{last_dt}' order by event_date")

    batch_count = 0

    while True:
        records = src_cursor.fetchmany(size=int(Variable.get("postgres.batch.size")))
        if not records:
            logging.info(f"Обработка запроса завершена")
            break
        execute_values(tgt_cursor,
                    "INSERT INTO target.history VALUES %s on conflict do nothing",
                    records)

        tgt_conn.commit()

        batch_count += 1
        logging.info(f"Batch:\t{batch_count}")

    src_cursor.close()
    tgt_cursor.close()
    logging.info(f"Cursors is closed")

    src_conn.close()
    tgt_conn.close()
    logging.info(f"Conections is closed")

with DAG('Postgres2Postgres_v2', default_args=default_args, schedule_interval='@hourly', catchup=False) as dag:



    update_history = PythonOperator(
        task_id='update_history',
        provide_context=True, 
        python_callable=update_history,
        sla=timedelta(minutes=3)
    )

    update_history
