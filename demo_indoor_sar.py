# Standard Library
from os import path
from pprint import pprint
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


def make_path(file: list, option: str):
    global base_dir
    _path = base_dir
    for i in file:
        _path = path.join(_path, i)

    return f'python {_path} {option}'


def main(**kwargs):

    conf = kwargs['dag_run'].conf

    _from_date = conf.get('from_date')
    _from_date = conf.get('from_date')
    _to_date = conf.get('to_date')

    # Prioritize get from date and to date from agrument
    if not _from_date or not _to_date:
        _from_date, _to_date = make_thursday()

    option_string = f'-f "{_from_date}" -t "{_to_date}"'

    return option_string


report_dir = Variable.get('reports_dir')
base_dir = path.join(report_dir, 'rp_1373_indoor_sar')


################ DAG config section ################
default_args = {
        'retries': 0,
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
sync_pi_location = make_path(['indoor_sar_sync.py'], '-e prod')

collector = make_path(['indoor_sar_processor.py'], '-e prod -m write')

combiner = make_path(['indoor_sar_combine.py'], '-e prod -m write')

add_master_gdocs = make_path(['gdocs', 'create_indoor_gdocs.py'],
                             '-e prod -g all')

create_detail_gdocs = make_path(['gdocs', 'create_indoor_history.py'],
                                '-e prod -g all')

# ################ Operator section ################
optional = '{{ task_instance.xcom_pull(task_ids="sniff_data") }}'

sniff = PythonOperator(
    dag=dag,
    task_id="sniff_data",
    provide_context=True,
    python_callable=main
)

action_sync_data = BashOperator(
    dag=dag,
    task_id='sync_pi_location',
    bash_command=sync_pi_location
)

action_collect_data = BashOperator(
    dag=dag,
    task_id='collect_data',
    bash_command=collector + ' ' + optional
)

action_combine_data = BashOperator(
    dag=dag,
    task_id='combine_data',
    bash_command=combiner + ' ' + optional
)

action_add_master_gdocs = BashOperator(
    dag=dag,
    task_id='add_master_gdocs',
    bash_command=add_master_gdocs + ' ' + optional
)

action_create_detail_gdocs = BashOperator(
    dag=dag,
    task_id='create_detail_gdocs',
    bash_command=create_detail_gdocs + ' ' + optional
)

################ Control Flow section ################
action_sync_data
sniff >> action_collect_data >> action_combine_data >> action_add_master_gdocs\
>> action_create_detail_gdocs
