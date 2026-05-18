# dags/retailmart_revenue_v1.py

import sys

sys.path.insert(0, '/usr/local/airflow/include')

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from db_utils import init_db, drop_day, get_conn
from data_gen import generate_orders

DAG_VERSION = 'v1'


def task_generate_orders(**context):

    init_db()

    run_date = context['data_interval_start'].date()

    print(f'[generate] Generating orders for {run_date}')

    orders = generate_orders(
        for_date=run_date,
        n_orders=120
    )

    context['ti'].xcom_push(
        key='orders',
        value=orders
    )

    context['ti'].xcom_push(
        key='run_date',
        value=str(run_date)
    )

    print(f'[generate] {len(orders)} orders pushed to XCom')


def task_validate_orders(**context):

    orders = context['ti'].xcom_pull(
        task_ids='generate_orders',
        key='orders'
    )

    errors = []

    for o in orders:

        if o['amount'] <= 0:
            errors.append(
                f"order {o['order_id']}: non-positive amount"
            )

        if o['category'] not in (
            'Electronics',
            'Clothing',
            'Groceries'
        ):
            errors.append(
                f"order {o['order_id']}: unknown category"
            )

        if not o['customer_id']:
            errors.append(
                f"order {o['order_id']}: missing customer_id"
            )

    if errors:
        raise ValueError(
            'Validation failed:\n' + '\n'.join(errors)
        )

    print(
        f'[validate] All {len(orders)} orders passed validation'
    )


def task_load_to_staging(**context):

    ti = context['ti']

    orders = ti.xcom_pull(
        task_ids='generate_orders',
        key='orders'
    )

    run_date = ti.xcom_pull(
        task_ids='generate_orders',
        key='run_date'
    )

    drop_day(run_date)

    print(f'[staging] Cleared existing rows for {run_date}')

    conn = get_conn()

    conn.executemany(
        '''
        INSERT INTO raw_orders
        VALUES (
            :order_id,
            :order_date,
            :category,
            :amount,
            :customer_id
        )
        ''',
        orders
    )

    conn.commit()
    conn.close()

    print(
        f'[staging] Inserted {len(orders)} rows into raw_orders'
    )


def task_compute_revenue(**context):

    run_date = context['ti'].xcom_pull(
        task_ids='generate_orders',
        key='run_date'
    )

    conn = get_conn()

    cur = conn.cursor()

    cur.execute(
        '''
        SELECT
            category,
            ROUND(SUM(amount), 2) AS total_rev,
            COUNT(*) AS order_count
        FROM raw_orders
        WHERE order_date = ?
        GROUP BY category
        ''',
        (run_date,)
    )

    rows = cur.fetchall()

    conn.close()

    revenue = [
        {
            'category': r[0],
            'total_rev': r[1],
            'order_count': r[2]
        }
        for r in rows
    ]

    context['ti'].xcom_push(
        key='revenue',
        value=revenue
    )

    print(
        f'[compute] Revenue computed for {run_date}: {revenue}'
    )


def task_load_to_report(**context):

    ti = context['ti']

    run_date = ti.xcom_pull(
        task_ids='generate_orders',
        key='run_date'
    )

    revenue = ti.xcom_pull(
        task_ids='compute_revenue',
        key='revenue'
    )

    now = datetime.utcnow().isoformat()

    conn = get_conn()

    for r in revenue:

        conn.execute(
            '''
            INSERT OR REPLACE INTO daily_revenue
            (
                report_date,
                category,
                total_rev,
                order_count,
                dag_version,
                loaded_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (
                run_date,
                r['category'],
                r['total_rev'],
                r['order_count'],
                DAG_VERSION,
                now
            )
        )

    conn.commit()
    conn.close()

    print(
        f'[report] {len(revenue)} category rows loaded '
        f'for {run_date} [{DAG_VERSION}]'
    )


def task_audit_log(**context):

    ti = context['ti']

    run_date = ti.xcom_pull(
        task_ids='generate_orders',
        key='run_date'
    )

    revenue = ti.xcom_pull(
        task_ids='compute_revenue',
        key='revenue'
    )

    total_rows = sum(
        r['order_count']
        for r in revenue
    )

    conn = get_conn()

    conn.execute(
        '''
        INSERT OR REPLACE INTO pipeline_audit
        (
            run_id,
            dag_id,
            dag_version,
            exec_date,
            row_count,
            status,
            logged_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            context['run_id'],
            context['dag'].dag_id,
            DAG_VERSION,
            run_date,
            total_rows,
            'success',
            datetime.utcnow().isoformat()
        )
    )

    conn.commit()
    conn.close()

    print(
        f'[audit] Run {context["run_id"]} '
        f'logged: {total_rows} orders'
    )


with DAG(
    dag_id='retailmart_revenue_v1',
    description='RetailMart Daily Revenue Pipeline v1',
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    catchup=False,
    max_active_runs=3,
    default_args={
        'owner': 'data-team',
        'retries': 2,
        'retry_delay': timedelta(minutes=3),
        'email_on_failure': False,
    },
    tags=['retailmart', 'etl', 'revenue', DAG_VERSION],
) as dag:

    t1 = PythonOperator(
        task_id='generate_orders',
        python_callable=task_generate_orders,
    )

    t2 = PythonOperator(
        task_id='validate_orders',
        python_callable=task_validate_orders,
    )

    t3 = PythonOperator(
        task_id='load_to_staging',
        python_callable=task_load_to_staging,
    )

    t4 = PythonOperator(
        task_id='compute_revenue',
        python_callable=task_compute_revenue,
    )

    t5 = PythonOperator(
        task_id='load_to_report',
        python_callable=task_load_to_report,
    )

    t6 = PythonOperator(
        task_id='audit_log',
        python_callable=task_audit_log,
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6