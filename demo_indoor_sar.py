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


def make_thursday() -> tuple:
    """Because this dag must run from 2 thursday"""
    current_date = datetime.utcnow()
    week_day = current_date.weekday()

    # If execute date is not thursday the back to 1 week
    if week_day < 3:
        current_date = current_date - timedelta(days=7)

    # Create offset from current date
    offset = (week_day - 3) % 7

    # Get need data
    last_thursday = current_date - timedelta(days=offset)
    this_thursday = current_date + timedelta(days=6 - offset)

    # Format date
    last_thursday = last_thursday.strftime('%Y-%m-%d 00:00:00')
    this_thursday = this_thursday.strftime('%Y-%m-%d 23:59:59')

    return last_thursday, this_thursday


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
from_date, to_date = make_thursday()

sync_pi_location = path.join(base_dir, 'indoor_sar_sync.py')

collector = path.join(base_dir, 'indoor_sar_processor.py')

combiner = path.join(base_dir, 'indoor_sar_processor.py')

add_master_gdocs = path.join(base_dir, 'gdocs', 'create_indoor_gdocs.py')

create_detail_gdocs = path.join(base_dir, 'gdocs', 'create_indoor_history.py')

################ Operator section ################
action_sync_data = BashOperator(
    dag=dag,
    task_id='sync_pi_location',
    bash_command=f'python {sync_pi_location} -e prod '
                 f'-f "{from_date}" -t "{to_date}"'
)

action_collect_data = BashOperator(
    dag=dag,
    task_id='collect_data',
    bash_command=f'python {collector} -e prod -m write '
                 f'-f "{from_date}" -t "{to_date}"'
)

action_combine_data = BashOperator(
    dag=dag,
    task_id='combine_data',
    bash_command=f'python {combiner} -e prod -m write '
                 f'-f "{from_date}" -t "{to_date}"'
)

action_add_master_gdocs = BashOperator(
    dag=dag,
    task_id='add_master_gdocs',
    bash_command=f'python {add_master_gdocs} -e prod -g all '
                 f'-f "{from_date}" -t "{to_date}"'
)

action_create_detail_gdocs = BashOperator(
    dag=dag,
    task_id='create_detail_gdocs',
    bash_command=f'python {create_detail_gdocs} -e prod -g all '
                 f'-f "{from_date}" -t "{to_date}"'
)


################ Control Flow section ################
action_sync_data >> action_collect_data >> action_combine_data \
>> action_add_master_gdocs >> action_create_detail_gdocs
