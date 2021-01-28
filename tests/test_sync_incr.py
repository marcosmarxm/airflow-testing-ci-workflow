import os
import subprocess
import pandas as pd
from unittest import TestCase
from pandas._testing import assert_frame_equal
from airflow.providers.postgres.hooks.postgres import PostgresHook


AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME')


def insert_initial_data(tablename, hook):
    """This script will populate database with initial data to run job"""
    conn_engine = hook.get_sqlalchemy_engine()
    sample_data = pd.read_csv(f'{AIRFLOW_HOME}/data/{tablename}.csv')
    sample_data.to_sql(name=tablename, con=conn_engine, if_exists='replace', index=False)


def create_table(tablename, hook):
    filename = tablename.replace('stg_', '')
    sql_stmt = open(f'/opt/airflow/dags/sql/init/create_{filename}.sql').read()
    hook.run(sql_stmt.format(tablename=tablename))


def get_csv_sample_as_df(filename):
    return pd.read_csv(f'{AIRFLOW_HOME}/data/{filename}.csv')


def execute_dag(dag_id, execution_date):
    """Execute a DAG in a specific date this process wait for DAG run or fail to continue"""
    subprocess.run(["airflow", "dags", "backfill", "-s", execution_date, dag_id])


class TestDataTransfer(TestCase):

    def setUp(self):
        self.oltp_hook = PostgresHook('oltp')
        self.olap_hook = PostgresHook('olap')
        self.default_date = '2020-01-01'
        self.transaction_table = 'transactions'
        self.product_table = 'products'
        self.product_sales_table = 'product_sales'

    def test_compare_transactions_source_dest_must_be_equal(self):
        """ Check if data from source is transfer to dest db after run DAG """
        create_table('transactions', self.oltp_hook)
        insert_initial_data('transactions', self.oltp_hook)

        create_table('products', self.oltp_hook)
        insert_initial_data('products', self.oltp_hook)

        create_table('stg_transactions', self.olap_hook)
        create_table('stg_products', self.olap_hook)
        create_table('products_sales', self.olap_hook)

        execute_dag('products_sales_pipeline', self.default_date)

        result = self.olap_hook.get_pandas_df(f'select * from {self.transaction_table}')
        expected = get_csv_sample_as_df(self.transaction_table)

        assert_frame_equal(result, expected)
        assert len(result) == 6
