import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(
    os.path.abspath(__file__)), ".."))

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import dotenv

from config import PROCESSED_DATA_DIR

def load_data(
    server: str = None, 
    database: str = None, 
    username: str = None, 
    password: str = None, 
    table_name: str = None,
    data_resolution: str = '1d'
):
    """
    Load data from Parquet files to SQL Server using SQLAlchemy.
    """

    # SQLAlchemy engine
    driver = '{ODBC Driver 18 for SQL Server}'
    connection_string = "DRIVER={};SERVER={};DATABASE={};UID={};PWD={}"
    driver_string = "mssql+pyodbc:///?odbc_connect={}"
    conn_str = connection_string.format(
        driver, server, database, username, password
    )
    engine = sqlalchemy.create_engine(driver_string.format(conn_str))
    engine.connect()

    folder_path = os.path.join(
        PROCESSED_DATA_DIR, 
        data_resolution
    )
    df = pd.read_parquet(folder_path, engine='pyarrow')
    # Insert data into the table
    df.to_sql(
        table_name, 
        con=engine, 
        chunksize=10, 
        if_exists='append', 
        index=False
    )


if __name__ == "__main__":
    # Load environment variables
    dotenv.load_dotenv()

    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_NAME')
    username = os.getenv('DB_USER')
    password = os.getenv('DB_PASS')
    table_name = 'HPGstockprice_daily'

    load_data(server, database, username, password, table_name)
