from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

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
    
) as etl:

    extract = BashOperator(
        task_id='extract',
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
    extract >> transform_and_load

