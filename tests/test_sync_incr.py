import os
import subprocess
import logging
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
    hook.run(open(f'./sql/init/create_{tablename}.sql').read())


def get_csv_sample_as_df(filename):
    return pd.read_csv(f'{AIRFLOW_HOME}/data/{filename}.csv')


def execute_dag(dag_id, execution_date):
    """Execute a DAG in a specific date this process wait for DAG run or fail to continue"""
    subprocess.run(["airflow", "dags", "backfill", "-s", execution_date, dag_id])


class TestDataTransfer(TestCase):

    def setUp(self):
        self.oltp_hook = PostgresHook('source')
        self.olap_hook = PostgresHook('dest')
        self.default_date = '2020-01-02'
        self.transaction_table = 'transactions'
        self.product_table = 'products'
        self.product_sales_table = 'product_sales'

    def test_compare_transactions_source_dest_must_be_equal(self):
        """ Check if data from source is transfer to dest db after run DAG """
        logging.info("Setup OLTP database table & data")
        create_table(self.transaction_table, self.oltp_hook)
        create_table(self.product_table, self.oltp_hook)
        insert_initial_data(self.transaction_table, self.oltp_hook)
        insert_initial_data(self.product_table, self.oltp_hook)

        logging.info("Setup OLAP database tables")
        create_table(self.product_table, self.olap_hook)
        create_table(self.transaction_table, self.olap_hook)
        create_table(self.product_sales_table, self.olap_hook)

        logging.info("Execute DAG")
        execute_dag('product_sales_pipeline', self.default_date)

        logging.info("Start tests")
        result = self.dest_hook.get_pandas_df(f'select * from {self.transaction_table}')
        sample_data = get_csv_sample_as_df(self.transaction_table)

        assert_frame_equal(result, sample_data)
        assert len(result) == 6
