from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators import BashOperator

default_args = {
    'owner': 'hung',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['tien.hung@geoguard.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'error_wifi_access_point',
    description='Error Wifi Access Report',
    schedule_interval='00 12 * * 6',
    default_args=default_args,
)

templated_command = """
export PYTHONPATH={{ var.value.gc_report_libs }}:{{ var.value.prod_reports_dir }}/error_wifi_access_point
{{ var.value.py38 }} {{ var.value.prod_reports_dir }}/error_wifi_access_point/reports/{{ params.report_id }}/{{ params.report_id }}.py -e test
"""

mobile_ap = BashOperator(
    task_id='mobile_ap',
    bash_command=templated_command,
    params=dict(
        report_id='rp_1605'
    ),
    dag=dag)
mobile_ap.doc_md = """\
# RP-1605: Mobile Wifi APs Report (Moving)
"""

wifi_db_error = BashOperator(
    task_id='wifi_db_error',
    bash_command=templated_command,
    params=dict(
        report_id='rp_1608'
    ),
    dag=dag)
wifi_db_error.doc_md = """\
# RP-1608: WiFi Access Points database error Report
"""

exclude_updated_wifi_db_error = BashOperator(
    task_id='exclude_updated_wifi_db_error',
    bash_command=templated_command,
    params=dict(
        report_id='rp_2130'
    ),
    dag=dag)
wifi_db_error.doc_md = """\
# RP-2130: Auto exclude updated G/S db error wifi APs from the database
"""

build_csv_for_wifi_ap = BashOperator(
    task_id='build_csv_for_wifi_ap',
    bash_command=templated_command,
    params=dict(
        report_id='rp_2016'
    ),
    dag=dag)
build_csv_for_wifi_ap.doc_md = """\
# RP-2016: Create a cronjob to build the csv file for list of wifi APs
"""

# Main flow
[mobile_ap, wifi_db_error, exclude_updated_wifi_db_error] >> build_csv_for_wifi_ap