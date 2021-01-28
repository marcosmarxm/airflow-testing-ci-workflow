from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(default_args={'owner': 'airflow'},
     schedule_interval=None,
     start_date=days_ago(2),
     template_searchpath='/opt/airflow/sql/sales/',
     tags=['etl', 'analytics', 'sales'])
def products_sales_pipeline():

    execution_date = '{{ ds }}'

    @task()
    def transfer_oltp_olap(dest_table, sql, params=None):
        """Get records from OLTP and transfer to OLAP database"""
        oltp_hook = PostgresHook(postgres_conn_id='oltp')
        olap_hook = PostgresHook(postgres_conn_id='olap')
        data_extracted = oltp_hook.get_records(sql=sql, parameters=params)
        olap_hook.insert_rows(dest_table, data_extracted, commit_every=1000)

    delete_products_sales_exec_date = PostgresOperator(
        task_id='delete_products_sales_exec_date',
        postgres_conn_id='olap',
        sql='delete_products_sales_exec_date.sql'
    )

    join_transactions_products = PostgresOperator(
        task_id='join_transactions_products',
        postgres_conn_id='olap',
        sql='join_transactions_products.sql'
    )

    union_incremental_products_sales = PostgresOperator(
        task_id='union_staging_to_products_sales',
        postgres_conn_id='olap',
        sql='union_staging_to_products_sales.sql'
    )

    agg_sales_category = PostgresOperator(
        task_id='rebuild_agg_sales_category',
        postgres_conn_id='olap',
        sql='agg_sales_category.sql'
    )


    load_incremental_transactions_data = transfer_oltp_olap(
        dest_table='stg_transactions',
        sql='select * from transactions where "purchase_date" = %s',
        params=[execution_date]
    )
    load_full_products_data = transfer_oltp_olap(
        dest_table='stg_products',
        sql='select * from products'
    )
    [load_full_products_data, load_incremental_transactions_data, delete_products_sales_exec_date] >> join_transactions_products
    join_transactions_products >> union_incremental_products_sales
    union_incremental_products_sales >> agg_sales_category


dag_prod_sales_pipeline = products_sales_pipeline()
