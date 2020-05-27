from os import path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators import BashOperator, PythonOperator

# Get environment variable
antifrauds_dir = Variable.get('antifrauds_dir')

# Get abs path
mock_apps_script = path.join(
    antifrauds_dir, 'RP_573_Get_Mock_App', 'get_mock_apps_2_0_0.py'
)

mock_apps_ini = path.join(
    antifrauds_dir, 'RP_573_Get_Mock_App', 'get_mock_apps.ini'
)

app_detail_script = path.join(
    antifrauds_dir, 'AF_688_App_Detail', 'get_apps_info_1_1_0.py'
)

app_detail_ini = path.join(
    antifrauds_dir, 'AF_688_App_Detail', 'get_apps_info.ini'
)

seven_days_ago = datetime.combine(
    datetime.today() - timedelta(7), datetime.min.time()
)

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
    'antifraud_daily',
    default_args=default_args,
    schedule_interval='15 08 * * *'
)


get_mock_apps = BashOperator(
    dag=dag,
    task_id='get_mock_apps',
    bash_command=f"python {mock_apps_script} -e prod -c {mock_apps_ini}"
)

get_app_detail = BashOperator(
    dag=dag,
    task_id='get_app_detail',
    bash_command=f"python {app_detail_script} -m production "
                 f"-c {app_detail_ini}"
)

# Work flow
get_mock_apps >> get_app_detail
