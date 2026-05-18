from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime


def show_interval(**ctx):

    start = ctx['data_interval_start']
    end = ctx['data_interval_end']

    print(
        f'[catchup=True] interval: '
        f'{start.date()} -> {end.date()}'
    )


with DAG(
    dag_id='catchup_true_demo',
    start_date=datetime(2026, 4, 1),
    schedule='@daily',
    catchup=True,
    max_active_runs=5,
    tags=['demo', 'catchup', 'compare'],
) as dag:

    PythonOperator(
        task_id='show_interval',
        python_callable=show_interval,
    )