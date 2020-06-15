# Standard Library
from os import path
from datetime import datetime, timedelta

# Airflow library
from airflow import DAG
from airflow.models import Variable
from airflow.operators import BashOperator, PythonOperator

# Local library
from gc_dbc import dbc

seven_days_ago = datetime.combine(
    datetime.today() - timedelta(7), datetime.min.time()
)

report_dir = Variable.get('reports_dir')
base_dir = path.join(report_dir, 'rp_1373_indoor_sar')


################ DAG config section ################
default_args = {
        'retries': 1,
        'owner': 'max',
        'email_on_retry': False,
        'email_on_failure': False,
        'depends_on_past': False,
        'start_date': seven_days_ago,
        'email': ['duc@geoguard.com'],
        'retry_delay': timedelta(minutes=5)
}

dag = DAG('indoor_sar_worker', catchup=True, default_args=default_args)

################ Script section ################
sync_pi_location = path.join(base_dir, 'indoor_sar_sync.py')

collector = path.join(base_dir, 'indoor_sar_processor.py')

combiner = path.join(base_dir, 'indoor_sar_processor.py')

add_master_gdocs = path.join(base_dir, 'create_indoor_gdocs.py')

create_detail_gdocs = path.join(base_dir, 'create_indoor_history.py')

################ Operator section ################
action_sync_data = BashOperator(
    dag=dag,
    task_id='sync_pi_location',
    bash_command=f"python {sync_pi_location} -e prod"
)

action_collect_data = BashOperator(
    dag=dag,
    task_id='sync_pi_location',
    bash_command=f"python {collector} -e prod -m write"
)

action_combine_data = BashOperator(
    dag=dag,
    task_id='sync_pi_location',
    bash_command=f"python {combiner} -e prod -m write"
)

action_add_master_gdocs = BashOperator(
    dag=dag,
    task_id='sync_pi_location',
    bash_command=f"python {add_master_gdocs} -e prod -g all"
)

action_create_detail_gdocs = BashOperator(
    dag=dag,
    task_id='sync_pi_location',
    bash_command=f"python {create_detail_gdocs} -e prod -g all"
)


################ Control Flow section ################
action_sync_data >> action_collect_data >> action_combine_data \
>> action_add_master_gdocs >> action_create_detail_gdocs
