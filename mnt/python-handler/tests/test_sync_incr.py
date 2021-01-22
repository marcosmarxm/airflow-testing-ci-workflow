import pytest
import pandas as pd
from pandas._testing import assert_frame_equal
from sqlalchemy import create_engine
   

class TestDataTransfer:

    def test_transactions_compare_source_dest_data(self):
        """ Check destination database receive all data from source from dag """
        dest_conn = create_engine('postgresql+psycopg2://root:root@dest-db:5432/dest')
        dest_data = pd.read_sql('select * from transactions', con=dest_conn)
        sample_data = pd.read_csv('./data/sample_transactions.csv')   
        assert_frame_equal(sample_data, dest_data)

    def test_transactions_count_dest_entries(self):
        """ Check if rows are expected after run DAG """
        dest_conn = create_engine('postgresql+psycopg2://root:root@dest-db:5432/dest')
        data = pd.read_sql('select * from transactions', con=dest_conn)
        assert len(data) == 6
