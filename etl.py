import os
from datetime import datetime

import asyncio

from data_crawler.crawler import StockCrawler
from data_crawler.formater import format_data

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
    asyncio.run(crawler.run())

    #################### FORMAT DATA ####################
    format_data(RAW_DATA_DIR, DATA_DIR)


if __name__ == '__main__':
    run()
    


