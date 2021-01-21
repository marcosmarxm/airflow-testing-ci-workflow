import pytest
import pandas as pd
from sqlalchemy import create_engine
   

class TestDataTransfer:

    def test_transactions_compare_source_dest_data(self):
        """ Check destination database receive all data from source from dag """
        dest_conn = create_engine('postgresql+psycopg2://root:root@dest-db:5432/dest')
        dest_data = pd.read_sql('select * from transactions', con=dest_conn).to_dict(orient='records')
        sample_data = pd.read_csv('./data/sample_transactions.csv').to_dict(orient='records')        
        assert sample_data == dest_data

    def test_transactions_count(self):
        """ Check if rows are expected after run DAG """
        dest_conn = create_engine('postgresql+psycopg2://root:root@dest-db:5432/dest')
        data = pd.read_sql('select * from transactions', con=dest_conn)
        assert len(data) == 6
