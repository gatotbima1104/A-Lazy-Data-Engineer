from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id="firewatch_test",
    start_date=datetime(2025,1,1),
    catchup=False,
    schedule=None
)
def firewatch_test():
    @task
    def hello():
        print("This running")
    
    hello()
    
firewatch_test()