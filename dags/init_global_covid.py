from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

default_args = {
    'start_date': datetime(2022, 5, 20)
}
with DAG(
    'init_global_covid', 
    default_args=default_args,
    catchup=False,
    
) as etl:
    download_dataset = BashOperator(
        task_id='download_dataset',
        bash_command='/home/luan/projects/covid_dashboard/pipelines/global/init/extract.sh ',
        do_xcom_push=False,
        env={'HOME': '/home/luan'}
    )
    create_tables = BashOperator(
        task_id='create_tables',
        bash_command='/home/luan/projects/covid_dashboard/pipelines/global/init/bash_scr/create_tables.sh ',
        do_xcom_push=False,
        env={'HOME': '/home/luan'}
    )
    transform_and_load_to_postgresql = BashOperator(
        task_id='transform_and_load_to_postgresql',
        bash_command='/home/luan/projects/covid_dashboard/pipelines/global/init/bash_scr/transform.sh ',
        do_xcom_push=False,
        env={'HOME': '/home/luan'}
    )
    [download_dataset, create_tables] >> transform_and_load_to_postgresql


        
        