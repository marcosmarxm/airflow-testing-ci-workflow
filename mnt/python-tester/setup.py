import pandas as pd
from sqlalchemy import create_engine

db = create_engine('postgresql+psycopg2://root:root@source-db:5432/source')

pd.read_csv('./data/source_sample.csv').to_sql(name='test', con=db, if_exists='replace')
