import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(
    os.path.abspath(__file__)), ".."))

import json
from datetime import datetime

import asyncio
import aiohttp
from asyncio import AbstractEventLoop
from aiohttp import ClientTimeout

from typing import List, Dict

from utils.file_manipulator import write_to_file
from config import RAW_DATA_DIR


class StockCrawler(object):
    def __init__(
        self, 
        data_dir: str = None, 
        start_date: int = None, 
        end_date: int = None, 
        resolution: str = None, 
        symbol: str = None
    ) -> None:  
        """
        Initialize the crawler.
        ----------------
        Args:
            data_dir: str
                The folder to save the data.
            start_date: int
                The start date of the data to be crawled.
            end_date: int
                The end date of the data to be crawled.
            resolution: str
                The resolution of the data to be crawled.
                Valid intervals: [1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo]
            symbol: str
                The symbol of the data to be crawled.

        """
        self.data_dir = data_dir
        if not os.path.exists(self.data_dir):
            os.mkdir(self.data_dir)

        self.start_date = start_date
        self.end_date = end_date
        self.interval = resolution
        self.symbol = symbol
        self.headers = {
            'Content-Type': 'application/json',
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0'
            )
        }
        self._url = f'https://query1.finance.yahoo.com/v8/finance/chart/{self.symbol}'


    def generate_parameters(self) -> List:
        """
        Generate list of parameters for crawler
        The time period will be divided into months
        List of parameters will contain monthly parameters
        
        """
        list_params = []
        second_in_a_quater = 2592000 * 3
        for i in range(
            self.start_date, self.end_date, second_in_a_quater
        ):
            list_params.append({
                'period1': i,
                'period2': i + second_in_a_quater,
                'interval': self.interval,
                'symbol': self.symbol,
            })

        return list_params
    

    async def fetch_data(
        self, 
        parameter: Dict = None, 
        save_foler: str = None
    ) -> Dict:
        """
        Make an asynchronous HTTP GET request to the specified Parameter.
        API url -> https://query1.finance.yahoo.com/v8/finance/chart
        ----------------
        Args:
            parameter: Dict[str]
                A dictionary of parameters to be passed to the API.
            save_folder: str
                The folder to save the data.
        Returns:
            json_data: Dict
                A dictionary of data returned from the API.

        """
        json_data = None
        connector = aiohttp.TCPConnector(limit=100)

        async with aiohttp.ClientSession(
            connector=connector,
            headers=self.headers,
        ) as session:
            
            async with session.get(
                url=self._url, params=parameter,
            ) as response:
                await asyncio.sleep(0.001)
                json_data = await response.read()

            json_data = json.loads(json_data)

        if json_data['chart']['error'] != None:
            return None
        
        time_start = parameter['period1']
        time_end = parameter['period2']
        file_name = os.path.join(
            save_foler, f"{time_start}_to_{time_end}.json"
        )
        write_to_file(json_data, file_name)


    async def get_all_data(self, list_params: List) -> List:
        """
        Fetch data from URLs in batches of 100 requests.

        """
        results = []
        count = 0
        for i in range(0, len(list_params), 10):
            batch = list_params[i:i + 10]
            folder_names = os.path.join(
                self.data_dir, f'batch_{count}'
            )
            if not os.path.exists(folder_names):
                os.mkdir(folder_names)

            tasks = [
                self.fetch_data(
                    parameter=param, 
                    save_foler=folder_names
                ) for param in batch
            ]
            batch_results = await asyncio.gather(*tasks)
            results.extend(batch_results)
            count += 1

        return results
    
    
    async def run(self) -> None:
        """
        Run the crawler.

        """
        list_params = self.generate_parameters()
        await self.get_all_data(list_params)



if __name__ == '__main__':
    start_date = 1298067272
    end_date = int(datetime.now().timestamp())

    crawler = StockCrawler(
        data_dir=RAW_DATA_DIR,
        start_date=start_date,
        end_date=end_date,
        resolution='60m',
        symbol='AMZN'
    )
    asyncio.run(crawler.run())