import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(
    os.path.abspath(__file__)), "..", ".."))

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.papermill.operators.papermill import PapermillOperator

from config import MODELS_PATH


default_args = {
    'owner': 'binh.truong',
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    dag_id='run_etl_process',
    default_args=default_args,
    description=(
        'Run extract, transform, load for Stock data daily\n'
        'Run predictive analysis for Stock data daily\n'
        'Run trading strategy for Stock data daily\n'
    ),
    start_date=days_ago(1),  # Set to a specific start date if needed
    catchup=False,
    schedule_interval='@daily'
) as dag:
    ##############################################################################
    ################################ Get data ####################################
    ##############################################################################

    @task(task_id='get_raw_data_daily')
    def run_etl(**kwargs):
        from etl import run_etl
        from datetime import datetime

        execution_date = kwargs['execution_date']
        # Use execution_date to determine the start date
        start_date = int(execution_date.timestamp()) - 3600*24
        end_date = int(datetime.now().timestamp())

        symbol = 'AMZN'
        resolution = '1d'
        run_etl(
            symbol=symbol, 
            resolution=resolution, 
            start_date=start_date, 
            end_date=end_date
        )

    ##############################################################################
    ############################### Run models ###################################
    ##############################################################################
    
    predictive_analytic_task = PapermillOperator(
        task_id="predictive_analytics_daily_data",
        input_nb=os.path.join(MODELS_PATH, "predictive_analytics.ipynb"),
        output_nb=os.path.join(MODELS_PATH, "analysis_1d_out-{{ execution_date }}.ipynb"),
        parameters={"execution_date": "{{ execution_date }}"},
    )

    training_task = PapermillOperator(
        task_id="training_daily_data",
        input_nb=os.path.join(MODELS_PATH, "training_strategy.ipynb"),
        output_nb=os.path.join(MODELS_PATH, "training_1d_out-{{ execution_date }}.ipynb"),
        parameters={"execution_date": "{{ execution_date }}"},
    )
    

    run_etl() >> [predictive_analytic_task, training_task]
        