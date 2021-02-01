from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


def transfer_oltp_olap(**kwargs):
    """Get records from OLTP and transfer to OLAP database"""
    dest_table = kwargs.get('dest_table')
    sql = kwargs.get('sql')

    oltp_hook = PostgresHook(postgres_conn_id='oltp')
    olap_hook = PostgresHook(postgres_conn_id='olap')

    data_extracted = oltp_hook.get_records(sql=sql)
    olap_hook.insert_rows(dest_table, data_extracted, commit_every=1000)


with DAG(dag_id='products_sales_pipeline',
         default_args={'owner': 'airflow'},
         schedule_interval=None,
         start_date=days_ago(2)) as dag:

    execution_date = '{{ ds }}'

    load_full_products_data = PythonOperator(
        task_id='load_full_products',
        python_callable=transfer_oltp_olap,
        op_kwargs={
            'dest_table': 'products',
            'sql': 'select * from products',
        })

    load_incremental_transactions_data = PythonOperator(
        task_id='load_incremental_transactions',
        python_callable=transfer_oltp_olap,
        op_kwargs={
            'dest_table': 'transactions',
            'sql': 'select * from transactions where "purchase_date" = cast({{ ds }} as text)',
        })
