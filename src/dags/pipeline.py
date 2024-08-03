from airflow import DAG
from airflow.operators.python import PythonOperator

from s3_loader import s3_load_file
from vertica_connector import vertica_operator

from datetime import datetime, timedelta

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


    load_dds_hubs_script = '''INSERT INTO STV2024071525__DWH.h_users (
                                    hk_user_id,
                                    user_id,
                                    registration_dt,
                                    load_dt,
                                    load_src
                                )
                            SELECT
                                hash(U.id) AS hk_user_id,
                                U.id AS user_id,
                                U.registration_dt,
                                now() AS load_dt,
                                's3' AS load_src
                            FROM
                                STV2024071525__STAGING.users AS U
                                LEFT JOIN STV2024071525__STAGING.group_log AS GL ON GL.user_id = U.id
                            WHERE
                                hash(U.id) NOT IN (
                                    SELECT
                                        hk_user_id
                                    FROM
                                        STV2024071525__DWH.h_users
                                );

                            INSERT INTO
                                STV2024071525__DWH.h_groups (
                                    hk_group_id,
                                    group_id,
                                    registration_dt,
                                    load_dt,
                                    load_src
                                )
                            SELECT
                                hash(G.id) AS hk_group_id,
                                G.id AS group_id,
                                G.registration_dt,
                                now() AS load_dt,
                                's3' AS load_src
                            FROM
                                STV2024071525__STAGING.groups AS G
                                LEFT JOIN STV2024071525__STAGING.group_log AS GL ON GL.group_id = G.id
                            WHERE
                                hash(group_id) NOT IN (
                                    SELECT
                                        hk_group_id
                                    FROM
                                        STV2024071525__DWH.h_groups
                                );'''

    load_dds_hubs = PythonOperator(
        task_id='load_dds_hubs',
        python_callable=vertica_operator,
        op_kwargs={
                'script': load_dds_hubs_script
            }
    )


    load_dds_links_script = '''INSERT INTO STV2024071525__DWH.l_user_group_activity (
                                hk_l_user_group_activity,
                                hk_user_id,
                                hk_group_id,
                                load_dt,
                                load_src
                            )
                            SELECT DISTINCT
                                hash(hu.hk_user_id, hg.hk_group_id),
                                hu.hk_user_id,
                                hg.hk_group_id,
                                now() AS load_dt,
                                's3' AS load_src
                            FROM STV2024071525__STAGING.group_log AS GL
                            LEFT JOIN STV2024071525__DWH.h_users AS hu ON GL.user_id = hu.user_id
                            LEFT JOIN STV2024071525__DWH.h_groups AS hg ON GL.group_id = hg.group_id
                            WHERE hash(hu.hk_user_id, hg.hk_group_id) NOT IN (SELECT hk_l_user_group_activity FROM STV2024071525__DWH.l_user_group_activity);
                            '''

    load_dds_links = PythonOperator(
        task_id='load_dds_links',
        python_callable=vertica_operator,
        op_kwargs={
                'script': load_dds_links_script
            }
    )

    load_dds_satellites_script = '''TRUNCATE TABLE STV2024071525__DWH.s_auth_history;

                                    INSERT INTO STV2024071525__DWH.s_auth_history (
                                        hk_l_user_group_activity,
                                        user_id_from,
                                        event,
                                        event_dt,
                                        load_dt,
                                        load_src
                                    )
                                    SELECT DISTINCT
                                        la.hk_l_user_group_activity,
                                        GL.user_id_from,
                                        GL.event,
                                        GL.event_ts as event_dt,
                                        now() as load_dt,
                                        's3' as load_src
                                    FROM STV2024071525__DWH.l_user_group_activity AS la
                                    LEFT JOIN STV2024071525__DWH.h_users AS hu ON la.hk_user_id = hu.hk_user_id
                                    LEFT JOIN STV2024071525__DWH.h_groups AS hg ON la.hk_group_id = hg.hk_group_id
                                    LEFT JOIN STV2024071525__STAGING.group_log AS GL 
                                        ON GL.user_id = hu.user_id AND GL.group_id = hg.group_id;'''
                                        
    load_dds_satellites = PythonOperator(
        task_id='load_dds_satellites',
        python_callable=vertica_operator,
        op_kwargs={
                'script': load_dds_satellites_script
            }
    )
    
    staging_list >> load_dds_hubs >> load_dds_links >> load_dds_satellites