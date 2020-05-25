from os import path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators import BashOperator, PythonOperator

from gc_dbc import dbc

seven_days_ago = datetime.combine(
    datetime.today() - timedelta(7), datetime.min.time()
)

report_dir = Variable.get('reports_dir')
print(report_dir)

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

dag = DAG('indoor_sar_worker', default_args=default_args)


def test_db(*args, **kwargs):

    with dbc('nv') as session:
        rows = session.execute("""select 1""")
        print(rows.fetchall())
    return


script_path = path.join(report_dir, 'rp_1373_indoor_sar/indoor_sar_sync.py')
# =============================


run_remote = BashOperator(
   task_id='sync_pibeacon_location',
   bash_command=f"python {script_path} -e test",
   dag=dag)

run_this = PythonOperator(
      task_id='test_db',
      provide_context=True,
      python_callable=test_db,
      dag=dag)

# Work flow
run_this >> run_remote
