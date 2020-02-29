from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'wait_for_downstream': True,
    'depends_on_past': True,
    'start_date': datetime(2020, 2, 29),
    'email': ['sa.garanin@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

with DAG('ED_UPD_CommodityHistory', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    update_commodity_history = PostgresOperator(
        task_id='update_commodity_history',
        sql= """
           insert into commodity_history
                (select 
                    ts,
                    "header"->>'uploaderID' uploader_id,
                    to_timestamp(message->>'timestamp', 'YYYY-MM-DD''T''HH24:MI:SS''Z''') message_ts,
                    message->>'marketId' market_id,
                    message->>'systemName' system_name,
                    message->>'stationName' station_name,
                    lower(x."name") commodity_name,
                    x.stock::int stock, 
                    x.demand,
                    x."buyPrice"::int buy_price,
                    x."meanPrice"::int mean_price,
                    x."sellPrice"::int sell_price,
                    x."stockBracket"::int stock_bracket,
                    case when x."demandBracket" = '' then 0 else x."demandBracket"::int end demand_bracket
                from events e 
                    cross join lateral jsonb_to_recordset(e.message->'commodities') as x("name" text, stock text, demand text, "buyPrice" text, "meanPrice" text, "sellPrice" text, "stockBracket" text, "demandBracket" text)
                    where "schema" = 'https://eddn.edcd.io/schemas/commodity/3' and ts >= (select coalesce(max(ts), '1970-01-01'::timestamp) from commodity_history)
                order by ts
                ) on conflict do nothing;
        """,
        postgres_conn_id='postgres_db_ed',
        autocommit=True
    )

    refresh_commodity_history_mv = PostgresOperator(
        task_id='refresh_commodity_history_mv',
        sql= """
            REFRESH MATERIALIZED VIEW public.commodity_cube WITH DATA;
        """,
        postgres_conn_id='postgres_db_ed',
        autocommit=True
    )

    update_commodity_history >> refresh_commodity_history_mv
