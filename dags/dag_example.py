from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator


def transfer_oltp_olap(**kwargs):
    """Get records from OLTP and transfer to OLAP database"""
    dest_table = kwargs.get('dest_table')
    sql = kwargs.get('sql')
    params = kwargs.get('params')

    oltp_hook = PostgresHook(postgres_conn_id='oltp')
    olap_hook = PostgresHook(postgres_conn_id='olap')

    data_extracted = oltp_hook.get_records(sql=sql, parameters=params)
    olap_hook.insert_rows(dest_table, data_extracted, commit_every=1000)


with DAG(dag_id='products_sales_pipeline',
         default_args={'owner': 'airflow'},
         schedule_interval=None,
         template_searchpath='/opt/airflow/sql/sales/',
         start_date=days_ago(2)) as dag:

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
            'sql': 'select * from transactions where "purchase_date" = %s',
            'params': ['{{ ds }}']
        })

    join_transactions_products = PostgresOperator(
        task_id='join_transactions_products',
        postgres_conn_id='olap',
        sql='join_transactions_products.sql'
    )

    [load_full_products_data, load_incremental_transactions_data] >> join_transactions_products 
