from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import time
import random
import logging

default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
}

REGIONS = ['north', 'south', 'east', 'west', 'central']

def extract_regional_data(region: str, **context):
    """Simulate extracting data from a regional database."""
    logging.info(f'[EXTRACT] Starting extraction for region: {region}')
    sleep_time = random.uniform(2, 4)
    time.sleep(sleep_time)
    record_count = random.randint(500, 5000)
    logging.info(f'[EXTRACT] {region}: extracted {record_count} records in {sleep_time:.1f}s')
    context['ti'].xcom_push(key=f'{region}_count', value=record_count)
    return record_count


def transform_data(region: str, **context):
    """Simulate data transformation for a region."""
    count = context['ti'].xcom_pull(
        key=f'{region}_count',
        task_ids=f'extract_{region}',
    )
    logging.info(f'[TRANSFORM] Processing {count} records for {region}')
    time.sleep(random.uniform(1, 2))
    transformed = int(count * 0.95)
    logging.info(f'[TRANSFORM] {region}: {transformed} records after cleaning')
    return transformed


def load_to_warehouse(**context):
    """Load all regional data to central warehouse."""
    total = 0
    for region in REGIONS:
        count = context['ti'].xcom_pull(task_ids=f'transform_{region}')
        total += count or 0
        logging.info(f'[LOAD] Loaded {total} total records to warehouse')
        time.sleep(1)
    return total


with DAG(
    dag_id='parallel_etl_pipeline',
    default_args=default_args,
    description='Parallel ETL for 5 regional databases',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'parallel', 'localexecutor-demo'],
) as dag:
    start = EmptyOperator(task_id='pipeline_start')
    end = EmptyOperator(task_id='pipeline_end')

    load = PythonOperator(
        task_id='load_to_warehouse',
        python_callable=load_to_warehouse,
        provide_context=True,
    )

    for region in REGIONS:
        extract = PythonOperator(
            task_id=f'extract_{region}',
            python_callable=extract_regional_data,
            op_kwargs={'region': region},
            provide_context=True,
        )

        transform = PythonOperator(
            task_id=f'transform_{region}',
            python_callable=transform_data,
            op_kwargs={'region': region},
            provide_context=True,
        )

        start >> extract >> transform >> load >> end