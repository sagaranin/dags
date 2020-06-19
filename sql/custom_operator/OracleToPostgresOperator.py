from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.oracle_hook import OracleHook
from psycopg2.extras import DictCursor, execute_values

import logging  

class OracleToPostgresOperator(BaseOperator):

        @apply_defaults
        def __init__(
                self,
                name: str,
                oracle_conn_id: str,
                postgres_conn_id: str,
                batch_size: int,
                *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self.name = name
            self.oracle_conn_id = oracle_conn_id
            self.postgres_conn_id = postgres_conn_id
            self.batch_size = batch_size

        def execute(self, context):
            # создание подключений
            src_conn = OracleHook(oracle_conn_id=oracle_conn_id).get_conn()
            tgt_conn = PostgresHook(postgres_conn_id=postgres_conn_id).get_conn()

            # формирование select - запроса
            select_query = "select {fields} from {src_schema}.{src_table}".format(**kwargs)
            if kwargs['use_conditions']:
                select_query += " where {conditions}".format(**kwargs)
            logging.info(f"Run query: \"{select_query}\"")

            # открытие курсоров на чтение и запись
            src_cursor = src_conn.cursor("serverCursor")
            tgt_cursor = tgt_conn.cursor()
            src_cursor.execute(select_query)

            # очистка Stage-таблицы
            tgt_cursor.execute("truncate table {tgt_schema}.{target_table_prefix}{tgt_table}".format(**kwargs))

            # обработка результата запроса
            batch_count = 0
            while True:
                logging.info(f"Processing batch:\t{batch_count},\tsize: {batch_size}")
                records = src_cursor.fetchmany(batch_size)
                
                if records:
                    execute_values(  # вставка батча данных
                        tgt_cursor,
                        "INSERT INTO {tgt_schema}.stg_{tgt_table} ({fields}) VALUES %s".format(**kwargs),
                        records
                    )
                else:
                    logging.info("Передача данных из таблицы {src_schema}.{src_table} завершена".format(**kwargs))
                    break

                batch_count += 1
            
            tgt_conn.commit()

            tgt_cursor.close()
            src_cursor.close()
            src_conn.close()
            tgt_conn.close()