import os
from datetime import datetime

import asyncio
import dotenv

from data_crawler.crawler import StockCrawler
from data_crawler.formater import format_data

from data_loader.loader import load_data
from config import RAW_DATA_DIR, PROCESSED_DATA_DIR
from utils.file_manipulator import write_to_file


def run() -> None:

    if not os.path.exists(RAW_DATA_DIR):
        os.mkdir(RAW_DATA_DIR)
    if not os.path.exists(PROCESSED_DATA_DIR):
        os.mkdir(PROCESSED_DATA_DIR)

    #################### GET RAW DATA ####################
    start_date = int(datetime(1997, 1, 1).timestamp())
    end_date = int(datetime.now().timestamp())
    symbol = 'AMZN'
    resolution_all = ['1d', '60m']

    for resolution in resolution_all:
        raw_dir_by_res = os.path.join(RAW_DATA_DIR, resolution)

        crawler = StockCrawler(
            start_date=start_date,
            end_date=end_date,
            symbol=symbol,
            resolution=resolution,
            data_dir=raw_dir_by_res
        )
        t1 = datetime.now()
        print(f'Start crawling stock data with resolution {resolution}...')
        asyncio.run(crawler.run())
        print(f'Finished crawling in {datetime.now() - t1}')

        #################### FORMAT DATA ####################
        processed_data_by_res = os.path.join(PROCESSED_DATA_DIR, resolution)
        t2 = datetime.now()
        print('Start formating raw data...')
        format_data(raw_dir_by_res, processed_data_by_res)
        print(f'Finished formating in {datetime.now() - t2}')

    ##################### LOAD DATA #####################
    # dotenv.load_dotenv()

    # server = os.getenv('DB_SERVER')
    # database = os.getenv('DB_NAME')
    # username = os.getenv('DB_USER')
    # password = os.getenv('DB_PASS')
    # driver= '{ODBC Driver 17 for SQL Server}'
    # table_name = 'HPG_stock_price'

    # t3 = datetime.now()
    # print('Start loading data to database...')
    # load_data(server, database, username, password, driver, table_name)
    # print(f'Finished loading in {datetime.now() - t3}')

if __name__ == '__main__':
    run()
    


