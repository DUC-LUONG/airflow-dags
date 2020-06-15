from airflow.models import Variable
from airflow.operators import BashOperator

from .gc_dag import GCDAG

dag = GCDAG(
    'RP-2099 New domain check',
    schedule_interval='00 8 * * *',
    email=["tien.hung@geoguard.com"]
)

# Base variable
gc_report_libs = Variable.get('gc_report_libs')
py38 = Variable.get('py38')

report_dir = Variable.get('prod_reports_dir')
script_dir = f"{report_dir}/rp_2099_migrate_new_domain"


def get_command(setting_file):
    """
    Get run command for RP-2099
    @param setting_file:
    @return:
    """
    return " && ".join(
        [
            f"export PYTHONPATH={gc_report_libs}",
            f"{py38}",
            f"{script_dir}/rp_2099.py {setting_file} "
            f"-e prod --draft n "
            f"--dry-run n",
        ]
    )


rp_2083 = BashOperator(
    task_id='RP_2083 ELS Gaming',
    bash_command=get_command(f"{script_dir}/rp_2083.ini"),
    dag=dag)

rp_1388 = BashOperator(
    task_id='RP_1388 North Carolina Lottery, North Dakota Lottery ('
            'Scientific Games) ',
    bash_command=get_command(f"{script_dir}/rp_1388.ini"),
    dag=dag)

cs_1285 = BashOperator(
    task_id='CS_1285 NorthCarolina Neogames',
    bash_command=get_command(f"{script_dir}/cs_1285.ini"),
    dag=dag)

rp_966 = BashOperator(
    task_id='RP_966 US4 Production',
    bash_command=get_command(f"{script_dir}/rp_966.ini"),
    dag=dag)

rp_1228 = BashOperator(
    task_id='RP_1228 NeoPollard',
    bash_command=get_command(f"{script_dir}/rp_1228.ini"),
    dag=dag)

# Main flow
[rp_2083, rp_1388, cs_1285, rp_966, rp_1228]