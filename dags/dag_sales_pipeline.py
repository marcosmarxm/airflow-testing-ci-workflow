import os
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


def render_sql_file(filepath):
    """Render a external file to be execute by a Airflow Hook"""
    return open(f'/opt/airflow/sql/{filepath}').read()


@dag(default_args={'owner': 'airflow'},
     schedule_interval=None,
     start_date=days_ago(2),
     tags=['etl', 'analytics', 'sales'])
def product_sales_pipeline():

    # yes this is how to get execution date...
    execution_date = '{{ ds }}'

    @task()
    def transfer_transaction_data(date: str):
        """Get records from transaction table for a specific date"""
        print(f'The exec date called is: {date}')
        oltp_hook = PostgresHook(postgres_conn_id='oltp')
        olap_hook = PostgresHook(postgres_conn_id='olap')
        data = oltp_hook.get_records(
            sql=render_sql_file('/sales/get_transaction.sql'),
            parameters={'date': date}
        )
        olap_hook.insert_rows('transactions', data, commit_every=1000)

    @task()
    def transfer_product_data():
        """Build entire product table everyday"""
        oltp_hook = PostgresHook(postgres_conn_id='oltp')
        olap_hook = PostgresHook(postgres_conn_id='olap')
        data = oltp_hook.get_records(
            sql=render_sql_file('/sales/get_product.sql')
        )
        olap_hook.insert_rows('products', data, commit_every=1000)

    delete_product_sales_exec_date = PostgresOperator(
        task_id='del_transaction_data_exec_date',
        postgres_conn_id='olap',
        sql='./sql/sales/delete_transactions_product.sql',
        params={'date': execution_date}
    )

    join_transaction_product = PostgresOperator(
        task_id='join_transaction_product_data',
        postgres_conn_id='olap',
        sql='./sql/sales/delete_transactions_product.sql',
    )

    union_incremental_product_sales = PostgresOperator(
        task_id='union_staging_data_product_sales',
        postgres_conn_id='olap',
        sql='./sql/sales/union_stagingtransaction_to_transaction.sql'
    )

    agg_sales_category_region = PostgresOperator(
        task_id='rebuild_agg_sales_category_region_table',
        postgres_conn_id='olap',
        sql='./sql/sales/agg_sales_category_region.sql'
    )

    agg_sales_category_top10 = PostgresOperator(
        task_id='rebuild_agg_sales_category_top10_table',
        postgres_conn_id='olap',
        sql='./sql/sales/agg_sales_category_top10.sql'
    )

    clean_stg_transactions = PostgresOperator(
        task_id='clean_stg_transaction_table',
        postgres_conn_id='olap',
        sql='./sql/sales/clean_stg_transaction.sql'
    )

    clean_stg_product = PostgresOperator(
        task_id='clean_stg_product_data',
        postgres_conn_id='olap',
        sql='./sql/sales/clean_stg_product.sql'
    )

    #execution_date >> delete_product_sales_exec_date
    load_incremental_transaction_data = transfer_transaction_data(execution_date)
    load_full_product_data = transfer_product_data()
    [load_full_product_data, load_incremental_transaction_data, delete_product_sales_exec_date] >> join_transaction_product
    join_transaction_product >> union_incremental_product_sales
    union_incremental_product_sales >> agg_sales_category_region
    union_incremental_product_sales >> agg_sales_category_top10
    union_incremental_product_sales >> clean_stg_transactions
    union_incremental_product_sales >> clean_stg_product


dag_prod_sales_pipeline = product_sales_pipeline()
