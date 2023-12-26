import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(
    os.path.abspath(__file__)), "..", ".."))

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import dotenv

from config import DATA_DIR

if not os.path.exists(DATA_DIR):
    os.mkdir(DATA_DIR)
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')


default_args = {
    'owner': 'binh.truong',
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    dag_id='run_etl_process',
    default_args=default_args,
    description='Run extract, transform, load for Stock data',
    start_date=days_ago(1),  # Set to a specific start date if needed
    catchup=False,
    schedule_interval='@daily'
) as dag:
    ##############################################################################################################
    ################################################## Get data ##################################################
    ##############################################################################################################

    @task(task_id='get_raw_data')
    def get_stock_data(**kwargs):
        from data_crawler.crawler import StockCrawler
        from datetime import datetime
        import asyncio

        execution_date = kwargs['execution_date']
        # Use execution_date to determine the start date
        start_date = int(execution_date.timestamp()) - 3600*24
        end_date = int(datetime.now().timestamp())
        symbol = 'HPG'

        crawler = StockCrawler(
            start_date=start_date,
            end_date=end_date,
            symbol=symbol,
            resolution='1D',
            data_dir=RAW_DATA_DIR
        )
        t1 = datetime.now()
        print('Start crawling stock data...')
        asyncio.run(crawler.run())
        print(f'Finished crawling in {datetime.now() - t1}')


    @task(task_id='format_raw_data')
    def format_data():
        from data_crawler.formater import format_data

        t2 = datetime.now()
        print('Start formating raw data...')
        format_data(RAW_DATA_DIR, DATA_DIR)
        print(f'Finished formating in {datetime.now() - t2}')


    @task(task_id='load_data_2_database')
    def load_transformed_data():
        from data_loader.loader import load_data

        dotenv.load_dotenv()
        server = os.getenv('DB_SERVER')
        database = os.getenv('DB_NAME')
        username = os.getenv('DB_USER')
        password = os.getenv('DB_PASS')
        driver= '{ODBC Driver 17 for SQL Server}'
        table_name = 'HPG_stock_price'

        t3 = datetime.now()
        print('Start loading data to database...')
        load_data(server, database, username, password, driver, table_name)
        print(f'Finished loading in {datetime.now() - t3}')

    # get_stock_data_task = get_stock_data('{{ execution_date }}')  # Pass execution_date to the task
    # format_data_task = format_data()
    # load_transformed_data = load_transformed_data()


    get_stock_data() >> format_data() >> load_transformed_data()
        