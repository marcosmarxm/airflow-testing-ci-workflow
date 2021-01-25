import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


def insert_data_sync_job(tablename):
    """This script will populate database with initial data to run job"""
    source = PostgresHook('source').get_sqlalchemy_engine()
    dest = PostgresHook('dest').get_sqlalchemy_engine()
    sample_data = pd.read_csv(f'/opt/airflow/data/sample_{tablename}.csv')
    sample_data.to_sql(name=tablename, con=source, if_exists='replace', index=False)
    sample_data.head(1).to_sql(name=tablename, con=dest, if_exists='replace', index=False)
