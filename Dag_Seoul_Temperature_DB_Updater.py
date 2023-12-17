from airflow import DAG
from airflow.models import Variable 
from airflow.decorators import task

from datetime import datetime, timedelta
from time import strftime, localtime

import socket
import requests
import pymysql


default_args = {
    'owner': 'tonydm', 
    'start_date': datetime(2023, 12, 1, hour=0, minute=00), 
    'end_date': datetime(2023, 12, 31, hour=23, minute=00), 
    'email': ['dmyang93@gmail.com'], 
    'retries': 2, 
    'retry_delay': timedelta(minutes=2),
}

def get_local_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 80))
    local_ip_address = s.getsockname()[0]
    s.close()
    
    return local_ip_address


def get_mysql_connect(autocommit=False):
    conn = pymysql.connect(
        host=get_local_ip_address(),
        user='airflow',
        password='1234',
        database='practice',
        charset='utf8',
        autocommit=autocommit
    )
    cursor = conn.cursor()
    return cursor


@task
def extract(api_url, api_key, lat, lon):
    response = requests.get(f'{api_url}?lat={lat}&lon={lon}&exclude=hourly,minutely&appid={api_key}&units=metric')
    json_content = response.json()
    
    return json_content['daily']
    
    
@task
def transform(raw_records):
    records = list()
    for raw_record in raw_records:
        date = strftime('%Y-%m-%d', localtime(raw_record['dt']))
        day_temp = raw_record['temp']['day']
        min_temp = raw_record['temp']['min']
        max_temp = raw_record['temp']['max']
        ngt_temp = raw_record['temp']['night']
        eve_temp = raw_record['temp']['eve']
        mrn_temp = raw_record['temp']['morn']
        records.append([date, min_temp, max_temp, mrn_temp, day_temp, eve_temp, ngt_temp])
        
    return records


@task
def load(records, schema, table):
    cur = get_mysql_connect()
    try:
        cur.execute(f'CREATE TEMPORARY TABLE temp AS SELECT * FROM {schema}.{table};')
        sql = (
            'INSERT INTO temp(date, min_temp, max_temp, morn_temp, day_temp, even_temp, nght_temp) '
            'VALUES (\"{}\", {}, {}, {}, {}, {}, {})'
        )
        for r in records:
            cur.execute(sql.format(r[0], r[1], r[2], r[3], r[4], r[5], r[6]))
        cur.execute('BEGIN;')
        cur.execute(f'DELETE FROM {schema}.{table};')
        sql = (
            'INSERT INTO {}.{} '
            'SELECT date, min_temp, max_temp, morn_temp, day_temp, even_temp, nght_temp, timestamp '
            'FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY timestamp DESC) seq FROM temp) t '
            'WHERE t.seq = 1;'
        ).format(schema, table)
        cur.execute(sql)
        cur.execute('COMMIT;')
    except(Exception, pymysql.DatabaseError) as error:
        cur.execute('ROLLBACK;')
        raise

    
with DAG(
    dag_id='SeoulTemperatureDBUpdater', 
    schedule='0 6 * * *', 
    catchup=False, 
    default_args=default_args
) as dag:
    api_url = Variable.get('weather_api_url')
    api_key = Variable.get('weather_api_key')
    lat, lon = 37.5665, 126.978
    
    schema = 'practice'
    table = 'seoul_temperature'
    
    records = transform(extract(api_url, api_key, lat, lon))
    load(records, schema, table)