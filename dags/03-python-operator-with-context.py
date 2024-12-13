from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'soyeong',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'end_date': datetime(2024, 1, 4)
}

def print_current_date_with_context(*args, **kwargs):
    '''
    kwargs: {'conf': <airflow.configuration.AirflowConfigParser object at 0x7f4bbcb98b20>,
    'dag': <DAG: python_dag_with_context>,
    'dag_run': <DagRun python_dag_with_context @ 2024-01-01 00:30:00+00:00: scheduled__2024-01-01T00:30:00+00:00,
    state:running,
    queued_at: 2024-12-10 13:30:40.259119+00:00. externally triggered: False>,
    'data_interval_end': DateTime(2024, 1, 2, 0, 30, 0, tzinfo=Timezone('UTC')), 'data_interval_start': DateTime(2024, 1, 1, 0, 30, 0, tzinfo=Timezone('UTC')),
    'ds': '2024-01-01', 'ds_nodash': '20240101', 'execution_date': <Proxy at 0x7f4bb6fd1a00 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f4bb6e268b0>, 'execution_date', DateTime(2024, 1, 1, 0, 30, 0, tzinfo=Timezone('UTC')))>, 'expanded_ti_count': None, 'inlets': [], 'logical_date': DateTime(2024, 1, 1, 0, 30, 0, tzinfo=Timezone('UTC')), 'macros': <module 'airflow.macros' from '/home/ohsoyoung-202012217/boostcamp/.venv/lib/python3.8/site-packages/airflow/macros/__init__.py'>, 'next_ds': <Proxy at 0x7f4bb6fd6140 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f4bb6e268b0>, 'next_ds', '2024-01-02')>, 'next_ds_nodash': <Proxy at 0x7f4bb6d5f300 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f4bb6e268b0>, 'next_ds_nodash', '20240102')>, 'next_execution_date': <Proxy at 0x7f4bb6d5f340 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f4bb6e268b0>, 'next_execution_date', DateTime(2024, 1, 2, 0, 30, 0, tzinfo=Timezone('UTC')))>, 'outlets': [], 'params': {}, 'prev_data_interval_start_success': None, 'prev_data_interval_end_success': None, 'prev_ds': <Proxy at 0x7f4bb6d5f380 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f4bb6e268b0>, 'prev_ds', '2023-12-31')>, 'prev_ds_nodash': <Proxy at 0x7f4bb6d5f3c0 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f4bb6e268b0>, 'prev_ds_nodash', '20231231')>, 'prev_execution_date': <Proxy at 0x7f4bb6d5f400 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f4bb6e268b0>, 'prev_execution_date', DateTime(2023, 12, 31, 0, 30, 0, tzinfo=Timezone('UTC')))>, 'prev_execution_date_success': <Proxy at 0x7f4bb6d5f440 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f4bb6e268b0>, 'prev_execution_date_success', None)>, 'prev_start_date_success': None, 'run_id': 'scheduled__2024-01-01T00:30:00+00:00', 'task': <Task(PythonOperator): print_current_date_with_context>, 'task_instance': <TaskInstance: python_dag_with_context.print_current_date_with_context scheduled__2024-01-01T00:30:00+00:00 [running]>, 'task_instance_key_str': 'python_dag_with_context__print_current_date_with_context__20240101', 'test_mode': False, 'ti': <TaskInstance: python_dag_with_context.print_current_date_with_context scheduled__2024-01-01T00:30:00+00:00 [running]>, 'tomorrow_ds': <Proxy at 0x7f4bb6d5f480 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f4bb6e268b0>, 'tomorrow_ds', '2024-01-02')>, 'tomorrow_ds_nodash': <Proxy at 0x7f4bb6d5f4c0 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f4bb6e268b0>, 'tomorrow_ds_nodash', '20240102')>, 'triggering_dataset_events': <Proxy at 0x7f4bb6d8ad80 with factory <function TaskInstance.get_template_context.<locals>.get_triggering_events at 0x7f4bb6d7aca0>>, 'ts': '2024-01-01T00:30:00+00:00', 'ts_nodash': '20240101T003000', 'ts_nodash_with_tz': '20240101T003000+0000', 'var': {'json': None, 'value': None}, 'conn': None, 'yesterday_ds': <Proxy at 0x7f4bb6d5f500 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f4bb6e268b0>, 'yesterday_ds', '2023-12-31')>, 'yesterday_ds_nodash': <Proxy at 0x7f4bb6d5f540 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f4bb6e268b0>, 'yesterday_ds_nodash', '20231231')>, 'templates_dict': None}
    '''
    print(f'kwargs: {kwargs}')
    execution_date = kwargs['ds']
    execution_date_nodash = kwargs['ds_nodash']
    print(f'excution_date_nodash: {execution_date_nodash}')
    execution_date = datetime.strptime(execution_date, '%Y-%m-%d').date()
    date_kor = ['월', '화', '수', '목', '금', '토', '일']
    datetime_weeknum = execution_date.weekday()
    print(f'{execution_date}는 {date_kor[datetime_weeknum]}요일입니다')

with DAG(
    dag_id='python_dag_with_context',
    default_args=default_args,
    schedule_interval='30 0 * * *',
    tags=['my_dags'],
    catchup=True
) as dag:
    
    PythonOperator(
        task_id='print_current_date_with_context',
        python_callable=print_current_date_with_context
    )