import boto3
import vertica_python


AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

SQL_SCRIPT = '''TRUNCATE TABLE STV2024071525__STAGING.{sql_key};
            COPY STV2024071525__STAGING.{sql_key}({sql_parameters})
            FROM LOCAL '/data/{sql_key}.csv'
            DELIMITER ','
            REJECTED DATA AS TABLE STV2024071525__STAGING.{sql_key}_rej;'''

SQL_PARAMETERS = {
    "groups": "id, admin_id, group_name, registration_dt, is_private",
    "users": "id, chat_name, registration_dt, country,age",
    "group_log": "group_id, user_id, user_id_from, event, event_ts"
}

CONN_INFO = {'host': 'vertica.tgcloudenv.ru', 
             'port': '5433',
             'user': 'stv2024071525',       
             'password': 'AAvjf6K2VVBEqhy',
             'database': 'dwh'
}

def s3_load_file(bucket: str, key: str):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=f'{key}.csv',
        Filename=f'/data/{key}.csv'
    )
    script = SQL_SCRIPT.format(sql_key=key, sql_parameters=SQL_PARAMETERS[key])
  
    with vertica_python.connect(**CONN_INFO) as connection:
        cur = connection.cursor()
        cur.execute(script)
        connection.commit()