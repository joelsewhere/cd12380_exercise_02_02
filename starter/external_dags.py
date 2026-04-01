from airflow.sdk import dag, task
from cd12380_exercise_02_02.starter.inventory_pipeline import stock_summary_asset, validation_asset


@dag(schedule=stock_summary_asset)
def finance_dag():

    @task
    def _():
        pass

@dag(schedule=validation_asset)
def logistics_dag():

    @task
    def _():
        pass

@dag
def anomaly_detection():

    @task
    def _():
        pass

finance_dag()
logistics_dag()
anomaly_detection()