
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    'owner': 'airflow',
}
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['extract_load', 'sales'])
def sync_source_dest_incremental():

    @task()
    def get_last_date_sync():
        dest_hook = PostgresHook(postgres_conn_id='dest')
        last_update_date = dest_hook.get_first(
            'select max("purchaseDate") from transactions'
        )[0]
        print(last_update_date)
        print(type(last_update_date))
        return last_update_date

    @task()
    def transfer_data_source_to_dest(last_update_date: str):
        source_hook = PostgresHook(postgres_conn_id='source')
        dest_hook = PostgresHook(postgres_conn_id='dest')

        data = source_hook.get_records(
            'select * from transactions where "purchaseDate" > %(last_update)s',
            parameters={'last_update': last_update_date}
        )
        dest_hook.insert_rows('transactions', data, commit_every=1000)


    last_update = get_last_date_sync()
    print(last_update)
    transfer_data_source_to_dest(last_update)
    

dag_sync_source_dest = sync_source_dest_incremental()
