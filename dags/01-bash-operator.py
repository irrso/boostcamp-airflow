from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'soyeong',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1)
}

with DAG(
    dag_id='bash_dag',
    default_args=default_args,
    schedule_interval='@once',
    tags=['my_dags']
) as dag:
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    t2 = BashOperator(
        task_id='sleep',
        bash_command='sleep 5',
        retries=2
    )

    t3 = BashOperator(
        task_id='pwd',
        bash_command='pwd'
    )

    t1 >> t2
    t1 >> t3
    # t1 >> [t2, t3]