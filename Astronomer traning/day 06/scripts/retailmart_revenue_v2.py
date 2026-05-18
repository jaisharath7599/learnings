# dags/retailmart_revenue_v2.py

import sys

sys.path.insert(0, '/usr/local/airflow/include')

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from db_utils import init_db, drop_day, get_conn
from data_gen import generate_orders

DAG_VERSION = 'v2'


def task_migrate_schema(**context):

    init_db()

    conn = get_conn()

    try:

        conn.execute(
            '''
            ALTER TABLE daily_revenue
            ADD COLUMN discount_total REAL DEFAULT 0.0
            '''
        )

        conn.commit()

        print(
            '[migrate] Added discount_total '
            'column to daily_revenue'
        )

    except Exception:

        print(
            '[migrate] Column already exists — skipping'
        )

    conn.close()


def task_generate_orders(**context):

    init_db()

    run_date = context['data_interval_start'].date()

    orders = generate_orders(
        for_date=run_date,
        n_orders=150
    )

    context['ti'].xcom_push(
        key='orders',
        value=orders
    )

    context['ti'].xcom_push(
        key='run_date',
        value=str(run_date)
    )

    print(
        f'[v2 generate] {len(orders)} '
        f'orders for {run_date}'
    )


def task_validate_orders(**context):

    orders = context['ti'].xcom_pull(
        task_ids='generate_orders',
        key='orders'
    )

    bad = [
        o for o in orders
        if o['amount'] <= 0 or not o['category']
    ]

    if bad:

        raise ValueError(
            f'{len(bad)} invalid orders detected'
        )

    print(
        f'[v2 validate] {len(orders)} orders valid'
    )


def task_ingest_to_staging(**context):

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
        f'[v2 ingest] {len(orders)} rows staged'
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
            COUNT(*) AS order_count,
            ROUND(SUM(amount) * 0.05, 2)
                AS discount_total
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
            'order_count': r[2],
            'discount_total': r[3],
        }
        for r in rows
    ]

    context['ti'].xcom_push(
        key='revenue',
        value=revenue
    )

    print(f'[v2 compute] {revenue}')


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
                discount_total,
                dag_version,
                loaded_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                run_date,
                r['category'],
                r['total_rev'],
                r['order_count'],
                r['discount_total'],
                DAG_VERSION,
                now
            )
        )

    conn.commit()
    conn.close()

    print(
        f'[v2 report] {len(revenue)} rows loaded '
        f'[{DAG_VERSION}]'
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
        f'[v2 audit] Run {context["run_id"]} '
        f'logged: {total_rows} orders'
    )


with DAG(
    dag_id='retailmart_revenue_v2',
    description='RetailMart Daily Revenue Pipeline v2',
    start_date=datetime(2026, 5, 1),
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

    t0 = PythonOperator(
        task_id='migrate_schema',
        python_callable=task_migrate_schema,
    )

    t1 = PythonOperator(
        task_id='generate_orders',
        python_callable=task_generate_orders,
    )

    t2 = PythonOperator(
        task_id='validate_orders',
        python_callable=task_validate_orders,
    )

    t3 = PythonOperator(
        task_id='ingest_to_staging',
        python_callable=task_ingest_to_staging,
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

    t0 >> t1 >> t2 >> t3 >> t4 >> t5 >> t6