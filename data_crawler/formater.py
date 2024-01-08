import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(
    os.path.abspath(__file__)), ".."))

import pandas as pd
import json
import shutil
from pathlib import Path
from typing import List, Dict
import pyarrow 


def process_json_file(file_path: str) -> List[Dict]:
    """
    Read and process raw json file.
    ----------
    Args:
        file_path: str
            Path to the json file.
    Returns:
        result: List[Dict]
            A list of dictionaries containing the data.

    """
    with open(file_path, 'r') as file:
        data = json.load(file)

    # if data['chart']['error'] == 'null':
    timestamp_column = data['chart']['result'][0]['timestamp']
    ohlcv_data = data['chart']['result'][0]['indicators']['quote'][0]
    # adjclose_data = data['chart']['result'][0]['indicators']['adjclose'][0]
    result = []
    for i in range(len(timestamp_column)):
        res = {
            'timestamp': timestamp_column[i],
            'open': ohlcv_data['open'][i],
            'close': ohlcv_data['close'][i],
            'high': ohlcv_data['high'][i],
            'low': ohlcv_data['low'][i],
            'volume': ohlcv_data['volume'][i],
            # 'adjclose': adjclose_data['adjclose'][i],
        }
        result.append(res)
    return result


def convert_to_parquet(data: List, raw_data_dir: str) -> None:
    """
    Convert data to_parquet format and save to data folder.
    Each file contains data of one month.
    File name format: YEAR_AND_MONTH=%Y%m_parquet
    ----------
    Args:
        data: List
            A list of data to be converted.
        raw_data_dir: str
            The folder to save the data.

    """
    df = pd.DataFrame(
        data, 
        columns=[
            'timestamp', 'open', 
            'close', 'high', 
            'low', 'volume',
        ]
    )
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    df['year_month'] = df['timestamp'].dt.strftime('%Y%m')

    for name, group in df.groupby('year_month'):
        group.drop('year_month', axis=1, inplace=True)
        file_path = os.path.join(
            raw_data_dir, f"YEAR_AND_MONTH={name}.parquet"
        )
        if os.path.exists(file_path):
            existing_df = pd.read_parquet(file_path)
            combined_df = pd.concat([existing_df, group], ignore_index=True)
            combined_df.to_parquet(
                file_path, index=False, engine='pyarrow'
            )
        else:
            group.to_parquet(
                file_path, index=False, engine='pyarrow'
            )


def format_data(raw_data_dir: str, save_dir: str) -> None:
    """
    Format data from raw json files to_parquet files.
    ----------
    Args:
        raw_data_dir: str
            The folder containing raw json files.
        save_dir: str
            The folder to save the data.

    """
    if not os.path.exists(save_dir):
        os.mkdir(save_dir)
        
    json_dir = Path(raw_data_dir)

    for json_file in json_dir.glob('*/*.json'):
        ohlcv_data = process_json_file(json_file)

        convert_to_parquet(ohlcv_data, save_dir)
    
    shutil.rmtree(raw_data_dir)
