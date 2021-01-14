
import json

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
}
@dag(default_args=default_args, schedule_interval='@daily', start_date=days_ago(2), tags=['example'])
def sync_source_dest_incremental():

    @task()
    def get_last_date_sync():

        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

        last_update_date = json.loads(data_string)
        return last_update_date


    @task(multiple_outputs=True)
    def transfer_data_source_to_dest(order_data_dict: dict):

        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}
    

    @task()
    def load(total_order_value: float):

        print("Total order value is: %.2f" % total_order_value)



    last_date = get_last_date_sync()
    order_summary = transfer_data_source_to_dest(last_date)
    load(order_summary["total_order_value"])
    sec_load = second_load(order_summary)
    after_second_load = func_after_second_load()
    sec_load >> after_second_load

sync_source_dest_incr_dag = sync_source_dest_incremental()
