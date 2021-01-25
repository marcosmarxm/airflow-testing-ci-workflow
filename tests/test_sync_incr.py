import pandas as pd
import subprocess
from unittest import TestCase
from pandas._testing import assert_frame_equal
from airflow.providers.postgres.hooks.postgres import PostgresHook

from setup_initial_env_state import insert_data_sync_job


def get_csv_sample_as_df(filename):
    return pd.read_csv(f'/opt/airflow/data/sample_{filename}.csv')


def execute_dag(dag_id, execution_date):
    """Execute a DAG in a specific date this process wait for DAG run or fail to continue"""
    subprocess.run(["airflow", "dags", "backfill", "-s", execution_date, dag_id])


class TestDataTransfer(TestCase):

    def setUp(self):
        self.dest_hook = PostgresHook('dest')
        self.source_hook = PostgresHook('source')
        self.default_date = '2020-01-02'
        self.transaction_table = 'transactions'

    def test_compare_transactions_source_dest_must_be_equal(self):
        """ Check if data from source is transfer to dest db after run DAG """
        insert_data_sync_job(self.transaction_table)
        execute_dag('sync_source_dest_incremental', self.default_date)
        result = self.dest_hook.get_pandas_df(f'select * from {self.transaction_table}')
        sample_data = get_csv_sample_as_df(self.transaction_table)

        assert_frame_equal(result, sample_data)
        assert len(result) == 6
