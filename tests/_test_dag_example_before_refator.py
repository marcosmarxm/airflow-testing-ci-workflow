import subprocess
import pandas as pd
from pandas._testing import assert_frame_equal
from airflow.providers.postgres.hooks.postgres import PostgresHook


class TestSalesPipeline:

    def test_validate_sales_pipeline(self):
        oltp_hook = PostgresHook('oltp')
        oltp_hook.run('''
        CREATE TABLE IF NOT EXISTS products (
            product_id       INTEGER,
            product_name     TEXT,
            product_category TEXT
        );  
        ''')
        oltp_conn = oltp_hook.get_sqlalchemy_engine()
        sample_data = pd.read_csv('./data/products.csv')
        sample_data.to_sql('products', con=oltp_conn, if_exists='replace', index=False)

        olap_hook = PostgresHook('olap')
        olap_hook.run('''
        CREATE TABLE IF NOT EXISTS products (
            product_id       INTEGER,
            product_name     TEXT,
            product_category TEXT
        );  
        ''')
        date = '2020-01-01'
        execute_dag('products_sales_pipeline', date)

        olap_product_size = olap_hook.get_records(
            'select * from products'
        )
        assert len(olap_product_size) == 5

        olap_product_data = olap_hook.get_pandas_df('select * from products')
        assert_frame_equal(olap_product_data, sample_data)
