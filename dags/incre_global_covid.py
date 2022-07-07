from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor 
from airflow import DAG
from datetime import datetime

with DAG(
    'incre_global_covid',
    start_date=datetime(2022, 7, 6),
    schedule_interval='@daily',
    catchup=False
) as pipeline:
    is_api_available = HttpSensor(
        task_id = 'is_api_available',
        http_conn_id='global_covid_api',
        endpoint='covid-19/countries?yesterday=yesterday'
    )
    update_date_table = BashOperator(
        task_id='update_date_table',
        bash_command='/home/luan/projects/covid_dashboard/pipelines/global/incre/update_date_table.sh ',
        do_xcom_push=False,
        env={'HOME': '/home/luan'},
        trigger_rule='all_success'
    )
    update_covid_table = BashOperator(
        task_id='update_covid_table',
        bash_command='/home/luan/projects/covid_dashboard/pipelines/global/incre/update_covid_table.sh ',
        do_xcom_push=False,
        env={'HOME': '/home/luan'},
        trigger_rule='all_success'
    )
    is_api_available >> update_date_table >> update_covid_table