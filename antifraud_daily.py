from os import path

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators import BashOperator, PythonOperator


seven_days_ago = datetime.combine(
    datetime.today() - timedelta(7), datetime.min.time()
)

current_date = datetime.utcnow()
yesterday = current_date - timedelta(days=1)
week_day = current_date.weekday()

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
    schedule_interval='@daily'
)


def check_weekday(*args, **kwargs):
    global week_day
    if week_day == 1:
        return
    exit(69)


check_weekday = PythonOperator(
    dag=dag,
    task_id='check_weekday',
    provide_context=True,
    python_callable=check_weekday
)

antifrauds_dir = Variable.get('antifrauds_dir')

mock_apps_script = path.join(
    antifrauds_dir, 'RP_573_Get_Mock_App', 'get_mock_apps_2_0_0.py'
)
mock_apps_ini = path.join(
    antifrauds_dir, 'RP_573_Get_Mock_App', 'get_mock_apps.ini'
)
get_mock_apps = BashOperator(
    dag=dag,
    xcom_push=True,
    task_id='get_mock_apps',
    bash_command=f'python {mock_apps_script} -e prod -c {mock_apps_ini} '
                 f'-f "{yesterday.strftime("%Y-%m-%d 00:00:00")}" '
                 f'-t "{yesterday.strftime("%Y-%m-%d 23:59:59")}"; echo $?'
)

app_detail_script = path.join(
    antifrauds_dir, 'AF_688_App_Detail', 'get_apps_info_1_1_0.py'
)
app_detail_ini = path.join(
    antifrauds_dir, 'AF_688_App_Detail', 'get_apps_info.ini'
)
get_app_detail = BashOperator(
    dag=dag,
    xcom_push=True,
    task_id='get_app_detail',
    trigger_rule=TriggerRule.ONE_SUCCESS,
    bash_command=f'python {app_detail_script} -m production '
                 f'-c {app_detail_ini} '
                 f'-f "{current_date.strftime("%Y-%m-%d 00:00:00")}" '
                 f'-t "{current_date.strftime("%Y-%m-%d 23:59:59")}"; echo $?'
)


app_to_test_script = path.join(
    antifrauds_dir, 'RP_901_Scrape_Apps', 'app_to_test_1_1_0.py'
)
app_to_test_ini = path.join(
    antifrauds_dir, 'RP_901_Scrape_Apps', 'app_to_test.ini'
)
app_to_test = BashOperator(
    dag=dag,
    xcom_push=True,
    task_id='app_to_test',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    bash_command=f"python {app_to_test_script} -c {app_to_test_ini}; echo $?"
)


web_scraper_script = path.join(
    antifrauds_dir, 'RP_901_Scrape_Apps', 'scrape_apps_1_2_0.py'
)
web_scraper_ini = path.join(
    antifrauds_dir, 'RP_901_Scrape_Apps', 'scrape_apps.ini'
)
web_scrapper = BashOperator(
    dag=dag,
    xcom_push=True,
    task_id='web_scrapper',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    bash_command=f"python {web_scraper_script} -c {web_scraper_ini} "
                 f"-m production; echo $?"
)

detect_change_script = path.join(
    antifrauds_dir, 'AF_1764_Detect_Change_Process',
    'detect_change_process_1_2_1.py'
)
detect_change_ini = path.join(
    antifrauds_dir, 'AF_1764_Detect_Change_Process',
    'detect_change_process.ini'
)
detect_change = BashOperator(
    dag=dag,
    xcom_push=True,
    task_id='detect_change',
    bash_command=f'python {detect_change_script} -c {detect_change_ini} '
                 f'-f "{yesterday.strftime("%Y-%m-%d")}" -e prod -r 2; echo $?'
)

appclonder_script = path.join(
    antifrauds_dir, 'RP_1280_App_Cloner',
    'detect_app_cloner_1_1_1.py'
)
appclonder_ini = path.join(
    antifrauds_dir, 'RP_1280_App_Cloner',
    'detect_app_cloner.ini'
)
app_cloner = BashOperator(
    dag=dag,
    xcom_push=True,
    task_id='app_cloner',
    bash_command=f'python {appclonder_script} -c {appclonder_ini} '
                 f'-d "{yesterday.strftime("%Y-%m-%d")}"; echo $?'
)


def conclusion(*args, **kargs):
    tasks = ('app_cloner', 'detect_change', 'get_mock_apps',
             'get_app_detail', 'app_to_test', 'web_scrapper')
    for task in tasks:
        err_code = task_instance.xcom_pull(task_ids=task)
        print(err_code)
    return


conclusion = PythonOperator(
    dag=dag,
    task_id='conclusion',
    provide_context=True,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    python_callable=conclusion
)

# Work flow
app_cloner >> conclusion
detect_change >> conclusion
get_mock_apps >> get_app_detail >> conclusion
check_weekday >> app_to_test >> web_scrapper >> conclusion
