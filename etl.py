import os
from datetime import datetime

import asyncio
import dotenv

from data_crawler.crawler import StockCrawler
from data_crawler.formater import format_data

from data_loader.loader import load_data
from config import DATA_DIR
from utils.file_manipulator import write_to_file


def run() -> None:

    if not os.path.exists(DATA_DIR):
        os.mkdir(DATA_DIR)
    RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')

    #################### GET RAW DATA ####################
    start_date = int(datetime(2012, 1, 1).timestamp())
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

    #################### FORMAT DATA ####################
    t2 = datetime.now()
    print('Start formating raw data...')
    format_data(RAW_DATA_DIR, DATA_DIR)
    print(f'Finished formating in {datetime.now() - t2}')

    ##################### LOAD DATA #####################
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

if __name__ == '__main__':
    run()
    


