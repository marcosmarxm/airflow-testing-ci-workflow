import os
import pandas as pd
from sqlalchemy import create_engine


def insert_initial_data(tablename):
    source = create_engine('postgresql+psycopg2://root:root@source-db:5432/source')
    dest = create_engine('postgresql+psycopg2://root:root@dest-db:5432/dest')
    sample_data = pd.read_csv(f'./data/sample_{tablename}.csv')
    sample_data.to_sql(name=tablename, con=source, if_exists='replace', index=False)
    sample_data.head(1).to_sql(name=tablename, con=dest, if_exists='replace', index=False)


insert_initial_data('transactions')
