import time
import requests
import json
import pandas as pd

from airflow.hooks.http_hook import HttpHook
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException


HHTP_CONN_ID = HttpHook.get_connection('http_conn_id')
API_KEY = HHTP_CONN_ID.extra_dejson.get('api_key')
BASE_URL = HHTP_CONN_ID.host
S3URL = 'https://storage.yandexcloud.net/s3-sprint3/cohort_'

postgres_conn_id = 'postgresql_de'

nickname = 'zhukov-an-an'
cohort = '1'
headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': API_KEY,
    'Content-Type': 'application/x-www-form-urlencoded'
}


def generate_report(ti):
    print('Making request generate_report')

    response = requests.post(f'{BASE_URL}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    if not task_id:
        raise AirflowException(f"Not managed to generate report : {response.content}")
    else:
        ti.xcom_push(key='task_id', value=task_id)
        print(f'Response is {response.content}')


def get_report(ti):
    print('Making request get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(6):
        response = requests.get(f'{BASE_URL}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        print(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if report_id:
        ti.xcom_push(key='report_id', value=report_id)
        print(f'Report_id={report_id}')
    else:
        raise AirflowException("Not managed to get report_id")

def get_core_report(filename, date, pg_table,ti, **context):
    report_id = ti.xcom_pull(key='report_id')

    load_id = context['execution_date']
    run_id = context['task_instance_key_str']

    if filename == 'user_order_log.csv':
        filename = 'user_orders_log.csv'
    
    s3_filename = f'{S3URL}{cohort}/{nickname}/project/{report_id}/{filename}'

    local_filename = '/lessons/' + date.replace('-', '') + '_' + filename
    response = requests.get(s3_filename)
    response.raise_for_status()
    
    with open(f"{local_filename}", "wb") as file:
        file.write(response.content)
    
    df = pd.read_csv(local_filename)
    if filename == 'customer_research.csv':
        pass
    else:
        df.drop_duplicates(subset=['id'])
    
    if filename == 'user_order_log.csv':
        if 'status' not in df.columns:
            df['status'] = 'shipped'
    
    df['load_id'] = str(load_id)
    df['run_id'] = run_id



    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(pg_table, engine , schema='staging', if_exists='append', index=False)


def get_increment(date, ti):
    print('Making request get_increment')
    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{BASE_URL}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=headers)
    response.raise_for_status()
    print(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']

    if increment_id:
        ti.xcom_push(key='increment_id', value=increment_id)
        print(f'increment_id={increment_id}')
    else:
        raise AirflowException("Not managed to get increment_id")



def upload_data_to_staging(filename, date, pg_table, pg_schema, ti, **context):
    load_id = context['execution_date']

    run_id = context['task_instance_key_str']
    
    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'{S3URL}{cohort}/{nickname}/project/{increment_id}/{filename}'

    print(s3_filename)

    local_filename = date.replace('-', '') + '_' + filename

    response = requests.get(s3_filename)
    with open(f"{local_filename}", "wb") as file:
        file.write(response.content)

    df = pd.read_csv(local_filename)
    df.drop_duplicates(subset=['id'])

    if 'status' not in df.columns:
        df['status'] = 'shipped'

    df['load_id'] = str(load_id)
    df['run_id'] = run_id


    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
