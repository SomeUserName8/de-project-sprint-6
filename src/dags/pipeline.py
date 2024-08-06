from airflow import DAG
from airflow.operators.python import PythonOperator

from s3_loader import s3_load_file
from vertica_connector import vertica_operator

from datetime import datetime, timedelta
from pathlib import Path

default_args = {
    'retries' : 3,
    'retry_delay' : timedelta(minutes=1)
}

with DAG(
    dag_id='pipeline',
    start_date=datetime(2024, 8, 3),
    catchup=False,
    default_args=default_args
) as dag:


    staging_list = list()
    for i in ['group_log', 'groups', 'users']:
        staging_list.append(PythonOperator(
            task_id=f'load_{i}',
            python_callable=s3_load_file,
            op_kwargs={
                    'bucket': 'sprint6',
                    'key': f'{i}',
                }
        ))


    load_dds_hubs = PythonOperator(
        task_id='load_dds_hubs',
        python_callable=vertica_operator,
        op_kwargs={
                'script': Path('/lessons/dags/SQL/load_dds_hubs_script.sql').read_text()
            }
    )



    load_dds_links = PythonOperator(
        task_id='load_dds_links',
        python_callable=vertica_operator,
        op_kwargs={
                'script': Path('/lessons/dags/SQL/load_dds_links_script.sql').read_text()
            }
    )
                                        
    load_dds_satellites = PythonOperator(
        task_id='load_dds_satellites',
        python_callable=vertica_operator,
        op_kwargs={
                'script': Path('/lessons/dags/SQL/load_dds_satellites_script.sql').read_text()
            }
    )
    
    staging_list >> load_dds_hubs >> load_dds_links >> load_dds_satellites