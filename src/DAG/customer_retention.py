from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from functions.core import generate_report, get_report, get_increment, upload_data_to_staging, get_core_report
from datetime import datetime, timedelta


args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}'


files = ['user_order_log', 'customer_research', 'user_activity_log']

with DAG(
        'Customer_retention_dag',
        default_args=args,
        description='Dag in response of customer retenetion calculation',
        catchup=True,
        start_date=datetime.today() - timedelta(days=8),
        end_date=datetime.today() - timedelta(days=1),
) as dag:

    sql_creation_mart = []
    operators_to_get_files = []

    for file in files:
        sql_creation_mart.append(PostgresOperator(task_id=f'create_staging_{file}',
        postgres_conn_id='postgresql_de',
        sql=f"sql/staging.{file}.sql"
    ))

        operators_to_get_files.append(PythonOperator(
        task_id=f'get_core_report_{file}',
        python_callable=get_core_report,
        op_kwargs={'filename': f'{file}.csv',
        'pg_table': f'temp_{file}',
        'date': business_dt}))

    generate_temp_marts = PostgresOperator(
        task_id='generate_temp_marts',
        postgres_conn_id='postgresql_de',
        sql="sql/staging.temp.sql")

    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt})

    upload_user_order_inc = PythonOperator(
        task_id='upload_user_order_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_orders_log_inc.csv',
                   'pg_table': 'temp_user_order_log',
                   'pg_schema': 'staging'})

    refresh_staging = PostgresOperator(
        task_id='refresh_staging',
        postgres_conn_id='postgresql_de',
        sql="sql/refresh_staging.sql")

    update_d_item_table = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id='postgresql_de',
        sql="sql/mart.d_item.sql")

    update_d_customer_table = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id='postgresql_de',
        sql="sql/mart.d_customer.sql")

    update_d_city_table = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id='postgresql_de',
        sql="sql/mart.d_city.sql")

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id='postgresql_de',
        sql="sql/mart.f_sales.sql",
        parameters={"date": {business_dt}}
    )

    update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id='postgresql_de',
        sql="sql/mart.f_customer_retention.sql"
    )


    (
            generate_temp_marts
            >> sql_creation_mart
            >> generate_report
            >> get_report
            >> operators_to_get_files 
            >> get_increment
            >> upload_user_order_inc
            >> refresh_staging
            >> [update_d_item_table, update_d_city_table, update_d_customer_table]
            >> update_f_sales 
            >> update_f_customer_retention
    )
