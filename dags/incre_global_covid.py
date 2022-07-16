from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow import DAG
from datetime import datetime, timedelta
import requests

def _is_api_available():
    if requests.get('https://disease.sh/v3/covid-19/countries?yesterday=yesterday').ok:
        return
    else:
        raise ValueError("API is not available")

default_args={
        "retries": 10,
        "retry_delay": timedelta(minutes=60),
        "email": ["non.static.structure@gmail.com"]
}
with DAG(
    'incre_global_covid',
    start_date=datetime(2022, 7, 6),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
) as pipeline:
    is_api_available = PythonOperator(
        task_id = 'is_api_available',
        python_callable=_is_api_available
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
