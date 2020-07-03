import psycopg2
from clickhouse_driver import connect as ch_connect
from psycopg2 import connect as pg_connect
from contextlib import closing

pg = pg_connect(host="db.larnerweb.ru", database="wiki", user="postgres", password="Ralf@12358")
ch = ch_connect('clickhouse://default:1598246@db.larnerweb.ru')

with closing(pg) as src_conn:
    src_cursor = src_conn.cursor("serverCursor")
    src_cursor.execute("select * from events_current")

    tgt_cursor = ch.cursor()

    while True:
        records = src_cursor.fetchmany(1000)
        # print([list(i) for i in records])
        tgt_cursor.executemany('INSERT INTO events_fact (meta_id,meta_dt,meta_request_id,meta_partition,meta_offset,uri,id,bot,`type`,namespace,`user`,title,comment,server_name,wiki,length_old,length_new,revision_old,revision_new) VALUES', 
            [list(i) for i in records])