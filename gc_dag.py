from typing import Optional, Union
from datetime import timedelta
from dateutil.relativedelta import relativedelta

from airflow import DAG
from airflow.utils.dates import days_ago

# Local lib
from gc_slack import Slacker
from settings import TEST, PRODUCTION


class GCDAG(DAG):
    @staticmethod
    def slack_notificaion(context):
        dag_id = context['task_instance'].dag_id
        status = context['task_instance'].state
        slacker = Slacker()
        slacker.message(
            text="Airflow task *{}* run status: *{}*".format(dag_id, status)
        )

    @staticmethod
    def get_default_args(
            notify_failue=True,
            notify_success=True,
            notify_retry=True
    ):
        default_args = {
            'owner': 'reporter',
            'depends_on_past': False,
            'start_date': days_ago(2),
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            # 'queue': 'bash_queue',
            # 'pool': 'backfill',
            # 'priority_weight': 10,
            # 'end_date': datetime(2016, 1, 1),
            # 'wait_for_downstream': False,
            # 'execution_timeout': timedelta(seconds=300),
            # 'dag': dag,
            # 'sla': timedelta(hours=2),
            # 'sla_miss_callback': GCDAG.slack_notificaion,
            # 'trigger_rule': 'all_success'
        }
        if notify_failue:
            default_args['on_failure_callback'] = GCDAG.slack_notificaion
        if notify_success:
            default_args['on_success_callback'] = GCDAG.slack_notificaion
        if notify_retry:
            default_args['on_retry_callback'] = GCDAG.slack_notificaion

    def __init__(
            self,
            environment: Optional[
                TEST, PRODUCTION
            ],
            title: str,
            description: str = '',
            schedule_interval: Optional[
                Union[str, timedelta, relativedelta]
            ] = timedelta(days=1),
            dag_default_args: dict = {},
            notify_failue=True,
            notify_success=True,
            notify_retry=True,
            *args,
            **kwargs
    ):

        if not dag_default_args:
            dag_default_args = GCDAG.get_default_args(
                notify_failue,
                notify_success,
                notify_retry
            )
        # Setup env
        self.environment = environment

        super().__init__(
            title,
            description=description,
            schedule_interval=schedule_interval,
            default_args=dag_default_args,
            *args, **kwargs
        )
