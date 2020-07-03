from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.clickhouse_hook import ClickHouseHook
from psycopg2.extras import DictCursor, execute_values
from contextlib import closing

import logging  


class PostgresToClickhouseOperator(BaseOperator):

        template_fields = ('select_query', 'insert_query')

        @apply_defaults
        def __init__(
                self,
                postgres_conn_id: str,
                clickhouse_conn_id: str,
                batch_size: int,
                select_query: str,
                insert_query: str,
                *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self.clickhouse_conn_id = clickhouse_conn_id
            self.postgres_conn_id = postgres_conn_id
            self.select_query = select_query
            self.insert_query = insert_query
            self.batch_size = batch_size


        def execute(self, context):
            logging.info(f"Execution query: {self.select_query}")
            # создание подключений
            with closing(PostgresHook(postgres_conn_id=self.postgres_conn_id).get_conn()) as src_conn:
                tgt_client =  ClickHouseHook(clickhouse_conn_id=self.clickhouse_conn_id).get_conn()
                
                src_cursor = src_conn.cursor("serverCursor")
                src_cursor.execute(self.select_query)

                count = 0
                while True:
                    logging.info(f"Batch {count}")
                    records = src_cursor.fetchmany(self.batch_size)
                    if records:
                        count += 1
                        tgt_client.execute(self.insert_query, [list(i) for i in records])
                    else:
                        break