from airflow.models import Variable
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
    'rp_2099',
    description='New domain check',
    schedule_interval='00 8 * * *',
    default_args=default_args,
)

# Base variable
gc_report_libs = Variable.get('gc_report_libs')
py38 = Variable.get('py38')

report_dir = Variable.get('prod_reports_dir')
script_dir = f"{report_dir}/rp_2099_migrate_new_domain"

templated_command = """
export PYTHONPATH={{ params.gc_report_libs }} 
 && {{ params.py38 }} {{ params.script_dir }}/rp_2099.py 
{{ params.setting_file }} -e prod 
"""

rp_2083 = BashOperator(
    task_id='rp_2083',
    bash_command=templated_command,
    params=dict(
        gc_report_libs=gc_report_libs,
        py38=py38,
        script_dir=script_dir,
        setting_file=f"{script_dir}/rp_2083.ini"
    ),
    dag=dag)
rp_2083.doc_md = """\
# RP_2083 ELS Gaming
"""

rp_1388 = BashOperator(
    task_id='rp_1388',
    bash_command=templated_command,
    params=dict(
        gc_report_libs=gc_report_libs,
        py38=py38,
        script_dir=script_dir,
        setting_file=f"{script_dir}/rp_1388.ini"
    ),
    dag=dag)
rp_1388.doc_md = """\
# RP_1388 North Carolina Lottery, North Dakota Lottery ('
'Scientific Games) 
"""

cs_1285 = BashOperator(
    task_id='cs_1285',
    bash_command=templated_command,
    params=dict(
        gc_report_libs=gc_report_libs,
        py38=py38,
        script_dir=script_dir,
        setting_file=f"{script_dir}/cs_1285.ini"
    ),
    dag=dag)
rp_1388.doc_md = """\
# CS_1285 NorthCarolina Neogames
"""

rp_966 = BashOperator(
    task_id='rp_966',
    bash_command=templated_command,
    params=dict(
        gc_report_libs=gc_report_libs,
        py38=py38,
        script_dir=script_dir,
        setting_file=f"{script_dir}/rp_966.ini"
    ),
    dag=dag)
rp_1388.doc_md = """\
# RP_966 US4 Production
"""

rp_1228 = BashOperator(
    task_id='rp_1228',
    bash_command=templated_command,
    params=dict(
        gc_report_libs=gc_report_libs,
        py38=py38,
        script_dir=script_dir,
        setting_file=f"{script_dir}/rp_1228.ini"
    ),
    dag=dag)
rp_1388.doc_md = """\
# RP_1228 NeoPollard
"""

# Main flow
[rp_2083, rp_1388, cs_1285, rp_966, rp_1228]
