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
    'rp_1605_mobile_ap',
    description='Mobile AP',
    schedule_interval='00 12 * * 6',
    default_args=default_args,
)

templated_command = """
export PYTHONPATH={{ var.value.gc_report_libs }}:{{ var.value.prod_reports_dir }}/error_wifi_access_point
{{ var.value.py38 }} {{ var.value.prod_reports_dir }}/error_wifi_access_point/reports/rp_1605/rp_1605.py -e test
"""

mobile_ap = BashOperator(
    task_id='mobile_ap',
    bash_command=templated_command,
    dag=dag)
mobile_ap.doc_md = """\
# RP-1605: Mobile Wifi APs Report (Moving)
"""