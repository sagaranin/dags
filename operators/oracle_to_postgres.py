from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.oracle_hook import OracleHook
from psycopg2.extras import DictCursor, execute_values

import logging  

class OracleToPostgresOperator(BaseOperator):

        ui_color = '#dee1ff'
        ui_fgcolor = '#000c0a'

        @apply_defaults
        def __init__(
                self,
                oracle_conn_id: str,
                postgres_conn_id: str,
                batch_size: int,
                params: dict,
                *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self.oracle_conn_id = oracle_conn_id
            self.postgres_conn_id = postgres_conn_id
            self.batch_size = batch_size
            self.params = params

        def execute(self, context):
            # создание подключений
            src_conn = OracleHook(oracle_conn_id=self.oracle_conn_id).get_conn()
            tgt_conn = PostgresHook(postgres_conn_id=self.postgres_conn_id).get_conn()

            # формирование select - запроса
            select_query = "select {fields} from {src_schema}.{src_table}".format(**self.params)
            if self.params['use_conditions']:
                select_query += " where {conditions}".format(**self.params)
            logging.info(f"Run query: \"{select_query}\"")

            # открытие курсоров на чтение и запись
            src_cursor = src_conn.cursor("serverCursor")
            tgt_cursor = tgt_conn.cursor()
            src_cursor.execute(select_query)

            # очистка Stage-таблицы
            tgt_cursor.execute("truncate table {tgt_schema}.{target_table_prefix}{tgt_table}".format(**self.params))

            # обработка результата запроса
            batch_count = 0
            while True:
                logging.info(f"Processing batch:\t{batch_count},\tsize: {self.batch_size}")
                records = src_cursor.fetchmany(self.batch_size)
                
                if records:
                    execute_values(  # вставка батча данных
                        tgt_cursor,
                        "INSERT INTO {tgt_schema}.stg_{tgt_table} ({fields}) VALUES %s".format(**self.params),
                        records
                    )
                else:
                    logging.info("Передача данных из таблицы {src_schema}.{src_table} завершена".format(**self.params))
                    break

                batch_count += 1
            
            tgt_conn.commit()

            tgt_cursor.close()
            src_cursor.close()
            src_conn.close()
            tgt_conn.close()