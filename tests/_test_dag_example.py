import subprocess
import pandas as pd
from pandas._testing import assert_frame_equal
from airflow.providers.postgres.hooks.postgres import PostgresHook


def execute_dag(dag_id, execution_date):
    """Execute a DAG in a specific date this process wait for DAG run or fail to continue"""
    subprocess.run(["airflow", "dags", "backfill", "-s", execution_date, dag_id])


def insert_initial_data(tablename, hook):
    """This script will populate database with initial data to run job"""
    conn_engine = hook.get_sqlalchemy_engine()
    sample_data = pd.read_csv(f'/opt/airflow/data/{tablename}.csv')
    sample_data.to_sql(name=tablename, con=conn_engine, if_exists='replace', index=False)


def create_table(tablename, hook):
    sql_stmt = open(f'/opt/airflow/sql/init/create_{tablename}.sql').read()
    hook.run(sql_stmt)


def output_expected_as_df(filename):
    return pd.read_csv(f'/opt/airflow/data/expected/{filename}.csv')


def execute_dag(dag_id, execution_date):
    """Execute a DAG in a specific date this process wait for DAG run or fail to continue"""
    subprocess.run(["airflow", "dags", "backfill", "-s", execution_date, dag_id])


class TestSalesPipeline:

    def test_validate_sales_pipeline(self):
        oltp_hook = PostgresHook('oltp')
        create_table('products', oltp_hook)
        insert_initial_data('products', oltp_hook)

        olap_hook = PostgresHook('olap')
        create_table('products', olap_hook)

        date = '2020-01-01'
        execute_dag('products_sales_pipeline', date)

        olap_product_size = olap_hook.get_records('select * from products')
        assert len(olap_product_size) == 5

        expected_product_data = output_expected_as_df('products')
        olap_product_data = olap_hook.get_pandas_df('select * from products')
        assert_frame_equal(olap_product_data, expected_product_data)
