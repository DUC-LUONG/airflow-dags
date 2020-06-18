from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import DummyOperator, EmailOperator, PythonOperator


default_args = {
    'owner': 'max',
    'start_date': datetime(2020, 6, 17),
    'email_on_failure': True,
    'email_on_retry': True,
    'email': ['duc@geoguard.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@once'
}


def error_function():
    raise Exception('Something wrong')


with DAG('test_alert',
         default_args=default_args,
         catchup=False) as dag:

    wont_email = DummyOperator(
      task_id='wont_email')

    will_email = DummyOperator(
      task_id='will_email',
      email_on_failure=True)

    failing_task = PythonOperator(
        task_id='failing_task',
        python_callable=error_function)

    send_mail = EmailOperator(
        task_id="send_mail",
        to='duc@geoguard.com',
        subject='Test mail',
        html_content='<p> You have got mail! <p>')

# Work FLow
wont_email
will_email
failing_task
send_mail
