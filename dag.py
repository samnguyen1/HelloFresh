from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization

default_args = {
    'owner': 'Sam Nguyen',
    'depends_on_past': False,
    'email': ['samnguyen2166@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }
dag = DAG(
    'hellofresh',
    default_args=default_args,
    description='Retrieve HelloFresh weekly menu and flatten recipes into csv',
    schedule_interval=timedelta(days=7),
    start_date=days_ago(2),
    tags=['example'],
)
t1 = BashOperator(
    task_id='flattenDataToCsv',
    #bash_command='python3 /home/airflow/airflow/dags/scripts/HelloFresh.py',
    bash_command='python3 helloFresh.py',
    dag=dag)
