from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


def render_sql_file(filepath):
    """Render a external file to be execute by a Airflow Hook"""
    return open(f'/opt/airflow/dags/sql/{filepath}').read()


@dag(default_args={'owner': 'airflow'},
     schedule_interval=None,
     start_date=days_ago(2),
     template_searchpath='/opt/airflow/dags/sql/sales/',
     tags=['etl', 'analytics', 'sales'])
def products_sales_pipeline():

    # Access execution date from Airflow context variables
    # this is kind complicated Airflow stuff...
    # learn more about: [link]
    execution_date = '{{ ds }}'

    @task()
    def transfer_transactions_data(date: str):
        """Get records from transaction table for a specific date"""
        print(f'The exec date called is: {date}')
        execution_date = '{{ ds }}'
        oltp_hook = PostgresHook(postgres_conn_id='oltp')
        olap_hook = PostgresHook(postgres_conn_id='olap')
        data_extracted = oltp_hook.get_records(
            sql='select * from transactions where "purchaseDate" = %s',
            parameters=[execution_date]
        )
        olap_hook.insert_rows('stg_transactions', data_extracted, commit_every=1000)

    @task()
    def transfer_products_data():
        """Build entire product table everyday"""
        oltp_hook = PostgresHook(postgres_conn_id='oltp')
        olap_hook = PostgresHook(postgres_conn_id='olap')
        data_extracted = oltp_hook.get_records('select * from products')
        olap_hook.insert_rows('stg_products', data_extracted, commit_every=1000)

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

    clean_staging = PostgresOperator(
        task_id='clean_staging',
        postgres_conn_id='olap',
        sql='clean_staging_tables.sql'
    )

    load_incremental_transactions_data = transfer_transactions_data(execution_date)
    load_full_products_data = transfer_products_data()
    [load_full_products_data, load_incremental_transactions_data, delete_products_sales_exec_date] >> join_transactions_products
    join_transactions_products >> union_incremental_products_sales
    union_incremental_products_sales >> agg_sales_category
    union_incremental_products_sales >> clean_staging


dag_prod_sales_pipeline = products_sales_pipeline()
