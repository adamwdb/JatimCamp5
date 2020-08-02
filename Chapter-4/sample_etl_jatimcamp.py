from datetime import timedelta
from datetime import datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from Dependencies import credentials

import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

def etl_jatimcamp():
    # DB GCP JATIMCAMP 5 DWH
    conn_dwh = sqlalchemy.create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.
                                               format(credential_db.db_jatimcamp5_DWH_username,
                                                      credential_db.db_jatimcamp5_DWH_password,
                                                      credential_db.db_jatimcamp5_DWH_host,
                                                      credential_db.db_jatimcamp5_DWH_name))

    # DB GCP JATIMCAMP 5 PRODUCTION
    conn = sqlalchemy.create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.
                                               format(credential_db.db_jatimcamp5_username,
                                                      credential_db.db_jatimcamp5_password,
                                                      credential_db.db_jatimcamp5_host,
                                                      credential_db.db_jatimcamp5_name))

    now = datetime.now()
    hour = now.hour()
    hour_after = hour + 1

    query = "select * from reviews where `index` between {} and {}".format(hour, hour_after)

    df_data_review = pd.read_sql(query,conn)

    df_data_review['time_adding'] = now.strftime('%Y-%M-%d')
    df_data_review['time_adding'] = pd.to_datetime(df_data_review['time_adding'], errors='coerce')

    df_data_review.to_sql('reviews_airflow', conn_dwh, if_exists='append')

    print('Done adding data into datawarehouse')


dag = DAG(
    'sample_etl_jatimcamp',
    default_args=default_args,
    description='A simple ETL for jatimcamp 5 DSI Jatim',
    schedule_interval='0 * * * *',
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
etl_production = PythonOperator(
    task_id='etl_production',
    python_callable=etl_jatimcamp,
    dag=dag,
)

etl_production