from gc_dag import GCDAG, GCJupyterOperator

dag = GCDAG(
    'demo_jupyter',
    description='New domain check',
    schedule_interval='00 8 * * *',
)

t1 = GCJupyterOperator(
    task_id='demo_jupyter',
    command='ls',
    dag=dag
)