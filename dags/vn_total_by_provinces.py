from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
import urllib.request

def check_splash_port():
    try:
        if urllib.request.urlopen("http://localhost:8050/").getcode() == 200:
            return ['crawl']
        return ['terminate']
    except:
        return ['terminate']

with DAG(
    'vn_total_by_provinces',
    start_date=datetime(2022, 7, 11),
    catchup=False,
    schedule_interval='@daily'
) as dag:
    is_splash_running = BranchPythonOperator(
        task_id='is_splash_running',
        python_callable=check_splash_port
    )
    crawl = BashOperator(
        task_id='crawl',
        bash_command='/home/luan/projects/covid_dashboard/pipelines/vn_etl/bash_scripts/vn_total_by_provinces/1_crawl.sh ',
        do_xcom_push=False
    )
    clean_and_validate_total = BashOperator(
        task_id='clean_and_validate_total',
        bash_command='/home/luan/projects/covid_dashboard/pipelines/vn_etl/bash_scripts/vn_total_by_provinces/2_clean_and_validate_total.sh ',
        do_xcom_push=False
    )
    load_to_postgresql = BashOperator(
        task_id='load_to_postgresql',
        bash_command='/home/luan/projects/covid_dashboard/pipelines/vn_etl/bash_scripts/vn_total_by_provinces/3_load_to_postgresql.sh ',
        do_xcom_push=False
    )
    terminate = DummyOperator(
        task_id='terminate'
    )
    is_splash_running >> crawl >> clean_and_validate_total >> load_to_postgresql >> terminate
    is_splash_running >> terminate