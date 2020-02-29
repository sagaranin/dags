from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'wait_for_downstream': True,
    'depends_on_past': True,
    'start_date': datetime(2020, 2, 29),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

with DAG('ED_UPD_ScanHistory', default_args=default_args, schedule_interval='@hourly', catchup=False) as dag:

    update_scan_history = PostgresOperator(
        task_id='update_scan_history',
        sql= """
            insert into scan_history 
            (select 
                ts,
                header->>'uploaderID' 			uploader_id,
                to_timestamp(header->>'gatewayTimestamp', 'YYYY-MM-DD''T''HH24:MI:SS''Z''') gateway_timestamp,
                (message->>'BodyID')::int8 					body_id,
                (message->>'MassEM')::decimal 				mass_em,
                (message->>'Radius')::decimal 				radius,
                (message->'StarPos'->>0)::decimal  			x,
                (message->'StarPos'->>1)::decimal 			y,
                (message->'StarPos'->>2)::decimal 			z,
                message->>'BodyName' 						body_name,
                (message->>'Landable')::bool		 		landable,
                message->>'ScanType' 						scan_type,
                (message->>'AxialTilt')::decimal 			axial_tilt,
                (message->>'Periapsis')::decimal 			periapsis,
                (message->>'TidalLock')::bool 				tilda_lock,
                message->>'Volcanism' 						volcanism,
                (message->>'WasMapped')::bool 				vas_mapped,
                message->>'Atmosphere' 						atmosphere,
                message->>'StarSystem' 						star_system,
                message->>'PlanetClass' 					planet_class,
                (message->>'Eccentricity')::decimal 			eccentricity,
                (message->>'OrbitalPeriod')::decimal 			orbital_period,
                (message->>'SemiMajorAxis')::decimal			semi_major_axis,
                (message->>'SystemAddress')::decimal			system_address,
                (message->>'WasDiscovered')::bool 			was_discovered,
                (message->>'RotationPeriod')::decimal 		rotation_period,
                (message->>'SurfaceGravity')::decimal  		surface_gravity,
                message->>'TerraformState' 					terraform_state,
                (message->>'SurfacePressure')::decimal  		surface_pressure,
                (message->>'OrbitalInclination')::decimal  	orbital_inclination,
                (message->>'SurfaceTemperature')::decimal  	surface_temperature,
                (message->>'DistanceFromArrivalLS')::decimal  distance_from_arrival_ls
            from events e where "schema" = 'https://eddn.edcd.io/schemas/journal/1' and message->>'event' = 'Scan' and ts >= (select coalesce(max(ts), '1970-01-01'::timestamp) from scan_history)
            order by ts) on conflict do nothing;
        """,
        postgres_conn_id='postgres_db_ed',
        autocommit=True
    )

    update_scan_history 
