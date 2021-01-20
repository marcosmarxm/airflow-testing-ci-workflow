import pytest
import pandas as pd
from sqlalchemy import create_engine
   

class TestDataTransfer:

    def test_transactions_sync_data(self):
        """ Check destination database receive all data from source from dag """
        dest_conn = create_engine('postgresql+psycopg2://root:root@dest-db:5432/dest')
        dest_data = pd.read_sql('select * from transactions', con=dest_conn).to_dict(orient='records')
        sample_data = pd.read_csv('./data/sample_transactions.csv').to_dict(orient='records')        
        assert sample_data == dest_data
