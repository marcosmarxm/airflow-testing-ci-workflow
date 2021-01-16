import pandas as pd
from sqlalchemy import create_engine

source = create_engine('postgresql+psycopg2://root:root@source-db:5432/source')
dest = create_engine('postgresql+psycopg2://root:root@dest-db:5432/dest')

pd.read_csv('./data/source_sample.csv').to_sql(name='transactions', con=source, if_exists='replace', index=False)
pd.read_csv('./data/source_sample.csv').head(1).to_sql(name='transactions', con=dest, if_exists='replace', index=False)