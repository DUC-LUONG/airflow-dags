from os import path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators import BashOperator, PythonOperator

# Get environment variable
antifrauds_dir = Variable.get('antifrauds_dir')

# Get abs path
mock_apps_path = path.join(antifrauds_dir, 'RP_573_Get_Mock_App')
app_detail_path = path.join(antifrauds_dir, 'AF_688_App_Detail')

default_args = {
        'retries': 1,
        'owner': 'max',
        'email_on_retry': True,
        'email_on_failure': True,
        'depends_on_past': False,
        'start_date': seven_days_ago,
        'email': ['duc@geoguard.com'],
        'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    'antifraud_dailt',
    default_args=default_args,
    schedule_interval='15 08 * * *'
)


get_mock_apps = BashOperator(
    dag=dag,
    task_id='get_mock_apps',
    bash_command=f"python "
                 f"{path.join(mock_apps_path, 'get_mock_apps_2_0_0.py')} "
                 f"-e prod"
                 f"-c {path.join(mock_apps_path, 'get_mock_apps.ini')} "
)

get_app_detail = BashOperator(
    dag=dag,
    task_id='get_app_detail',
    bash_command=f"python "
                 f"{path.join(app_detail_path, 'get_apps_info_1_1_0.py')} "
                 f"-e production"
                 f"-c {path.join(app_detail_path, 'get_apps_info.ini')}"
)



