from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests

def _check_splash_port():
    try:
        if requests.get("http://localhost:8050/").ok:
            return 
    except:
        raise ValueError('Splash is not running.')

extract_script = \
    '/home/luan/projects/covid_dashboard/pipelines/vn_etl/bash_scripts/last30days_etl/1_extract.sh'
transform_script = \
    '/home/luan/projects/covid_dashboard/pipelines/vn_etl/bash_scripts/last30days_etl/2_transformation.sh'

DAG_NAME = 'last30days_covid'
with DAG(
    DAG_NAME, 
    start_date=datetime(2022, 7, 7),
    schedule_interval='@daily',
    catchup=False,
    default_args={
        "retries": 10,
        "retry_delay": timedelta(minutes=60),
        "email": ["non.static.structure@gmail.com"]
    },
    
) as etl:
    is_splash_running = PythonOperator(
        task_id='is_splash_running',
        python_callable=_check_splash_port
    )
    crawl = BashOperator(
        task_id='crawl',
        bash_command=extract_script + ' ',
        do_xcom_push=False
    )
    transform_and_load = BashOperator(
        task_id='transform_and_load',
        bash_command=transform_script + ' ',
        do_xcom_push=False,
        env={'HOME': '/home/luan/'},
        trigger_rule='all_success'
    )
    is_splash_running >> crawl >> transform_and_load
