from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests as req
import json
import psycopg2
import time
import csv, sys

csv.field_size_limit(sys.maxsize)

default_args = {
    'owner': 'airflow',
    'wait_for_downstream': False,
    'depends_on_past': True,
    'start_date': datetime(2019, 1, 1, 3, 0, 0),
    'email': ['sa.garanin@gmail.com'],
    'email_on_success': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 12,
    'retry_delay': timedelta(hours=1)
}

def update(ds, **kwargs):
    conn = psycopg2.connect(dbname='autoru', user='postgres', password='Ralf@12358', host='db.larnerweb.ru')
    cursor = conn.cursor()

    for i in range(1, 100):
        try:
            url = f"https://auto.ru/{kwargs['region']}/cars/all/?page={i}&sort=cr_date-desc&output_type=list"
            response = req.get(url, timeout=60)
            response.encoding = 'UTF-8'

            soup = BeautifulSoup(response.text, 'lxml')
            elem = soup.find(id="initial-state")
            data = elem.contents[0]

            js = json.loads(data)

            offers = js['listing']['data']['offers']
            print(kwargs['region'], url, i)
            for offer in offers:
                # print(i, offer['id'], offer['hash'], offer['additional_info']['update_date'])
                cursor.execute("insert into offers (id, hash, last_update, json_data) values (%s, %s, %s, %s) on CONFLICT DO NOTHING",
                            (int(offer['id']), offer['hash'],  int(offer['additional_info']['update_date']), json.dumps(offer)))

            conn.commit()
        except Exception as ex:
            print(ex)

    conn.close()


with DAG('AUTORU_Update', default_args=default_args, schedule_interval='@hourly', catchup=False) as dag:

    update_flat = PostgresOperator(
        task_id='update_flat',
        sql= '''
            insert into offers_flat 
            select  
                (json_data->>'id')::bigint id,
                json_data->>'hash' hash,
                last_update,
                row_id,
            --	json_data->>'tags' tags,
                json_data->'seller'->>'name' seller_name,
                (json_data->'seller'->'location'->'coord'->>'latitude')::float8 seller_location_lat, 
                (json_data->'seller'->'location'->'coord'->>'longitude')::float8	seller_location_long,
                (json_data->'seller'->'location'->'region_info'->>'id')::int8 seller_region_id,
                json_data->'seller'->'location'->'region_info'->>'name' seller_region_name,
                (json_data->'seller'->'location'->'region_info'->>'latitude')::float8 region_location_lat, 
                (json_data->'seller'->'location'->'region_info'->>'longitude')::float8 region_location_long,
                (json_data->'price_info'->>'EUR')::int current_price_eur,
                (json_data->'price_info'->>'RUR')::int current_price_rur,
                (json_data->'price_info'->>'USD')::int current_price_usd,
                json_data->'price_info'->>'with_nds' price_nds,
                (json_data->'state'->>'mileage')::int8 mileage,
                json_data->>'status' status,
                json_data->>'section' "section",
                json_data->>'category' category,
                json_data->'documents'->>'pts' documents_pts,
                json_data->'documents'->>'vin' documents_vin,
                json_data->'documents'->>'year' documents_year,
                json_data->'documents'->>'pts_original' documents_pts_original,
                json_data->'documents'->>'license_plate' documents_license_plate,
                json_data->'documents'->>'owners_number' documents_owners_number,
                json_data->'documents'->>'custom_cleared' documents_custom_cleared,
                json_data->'documents'->>'vin_resolution' documents_vin_resolution,
                json_data->'documents'->'warranty_expire'->>'year' warranty_expire_year,
                json_data->'documents'->'warranty_expire'->>'month' warranty_expire_month,
                json_data->'documents'->'warranty_expire'->>'day' warranty_expire_day,
                json_data->'vehicle_info'->>'vendor' vendor,
                json_data->'vehicle_info'->'mark_info'->>'code' mark_code,
                json_data->'vehicle_info'->'mark_info'->>'name' mark_name,
                json_data->'vehicle_info'->'mark_info'->>'ru_name' mark_ru_name,
                json_data->'vehicle_info'->'model_info'->>'code' model_code,
                json_data->'vehicle_info'->'model_info'->>'name' model_name,
                json_data->'vehicle_info'->'model_info'->>'ru_name' model_ru_name,
                (json_data->'vehicle_info'->'super_gen'->>'id')::int8 super_gen_id,
                json_data->'vehicle_info'->'super_gen'->>'name' super_gen_name,
                json_data->'vehicle_info'->'super_gen'->>'year_from' super_gen_year_from,
                json_data->'vehicle_info'->'super_gen'->>'price_segment' super_gen_price_segment,
                (json_data->'vehicle_info'->'tech_param'->>'id')::int8  tech_param_id, 
                (json_data->'vehicle_info'->'tech_param'->>'power')::int8  tech_param_power,
                (json_data->'vehicle_info'->'tech_param'->>'fuel_rate')::float8  tech_param_fuel_rate,
                json_data->'vehicle_info'->'tech_param'->>'gear_type' tech_param_gear_type,
                (json_data->'vehicle_info'->'tech_param'->>'power_kvt')::int8  tech_param_power_kvt,
                json_data->'vehicle_info'->'tech_param'->>'human_name' tech_param_human_name,
                json_data->'vehicle_info'->'tech_param'->>'engine_type' tech_param_engine_type,
                (json_data->'vehicle_info'->'tech_param'->>'acceleration')::float8  tech_param_acceleration,
                (json_data->'vehicle_info'->'tech_param'->>'displacement')::int8  tech_param_displacement,
                json_data->'vehicle_info'->'tech_param'->>'transmission' tech_param_transmission,
                json_data->'vehicle_info'->'configuration'->>'body_type' body_type,
                json_data->'vehicle_info'->'configuration'->>'auto_class' auto_class,
                json_data->'vehicle_info'->'configuration'->>'doors_count' doors_count,
                json_data->'vehicle_info'->'configuration'->>'human_name' human_name,
                (json_data->'vehicle_info'->'configuration'->>'trunk_volume_max')::int8  trunk_volume_max,
                (json_data->'vehicle_info'->'configuration'->>'trunk_volume_min')::int8  trunk_volume_min,
                json_data->'vehicle_info'->>'steering_wheel' steering_wheel,
                to_timestamp((json_data->'additional_info'->>'creation_date')::bigint/1000) creation_date,
                to_timestamp((json_data->'additional_info'->>'update_date')::bigint/1000) last_update_date,
                (json_data->'additional_info'->'review_summary'->>'counter')::int4 review_counter,
                (json_data->'additional_info'->'review_summary'->>'avg_rating')::float8 review_avg_rating
            from offers
                where row_id >= (select coalesce(max(row_id), 0) from offers_flat)
            on conflict do nothing
        ''',
        postgres_conn_id='postgres_db_auto',
        autocommit=True
    )

    update_offers_actual = PostgresOperator(
        task_id='update_offers_actual',
        sql= '''
            truncate table offers_actual;
            insert into offers_actual
            select * from (
                select 
                    *,  
                    row_number() over (partition by id order by last_update_date desc) rn
                from offers_flat
                ) s
            where rn = 1
        ''',
        postgres_conn_id='postgres_db_auto',
        autocommit=True
    )

    update_flat >> update_offers_actual
    
    for region in ['moskovskaya_oblast', 'moskva', 'sankt-peterburg', 'vladimir', 'volgograd', 'voronezh', 'ekaterinburg', 'ivanovo', 'kazan', 'kaluga', 'kostroma', 'krasnodar', 'krasnoyarsk', 'nizhniy_novgorod', 'novosibirsk', 'omsk','perm','rostov-na-donu',
        'samara','saratov','tver','tula','ufa','chelyabinsk','yaroslavl', 'habarovsk', 'habarovskiy_kray']:

        load_offers = PythonOperator(
            task_id=f'load_updates_{region}',
            provide_context=True,
            python_callable=update,
            op_kwargs={'region': region}
        )

        load_offers >> update_flat
