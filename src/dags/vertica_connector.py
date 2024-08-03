import vertica_python

CONN_INFO = {'host': 'vertica.tgcloudenv.ru', 
             'port': '5433',
             'user': 'stv2024071525',       
             'password': 'AAvjf6K2VVBEqhy',
             'database': 'dwh'
}

def vertica_operator(script: str):  
    with vertica_python.connect(**CONN_INFO) as connection:
        cur = connection.cursor()
        cur.execute(script)
        connection.commit()