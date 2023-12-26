import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(
    os.path.abspath(__file__)), ".."))

import pandas as pd
import pyodbc
import dotenv

from config import DATA_DIR


def load_data(
    server: str = None, 
    database: str = None, 
    username: str = None, 
    password: str = None, 
    driver: str = None, 
    table_name: str = None
):
    """
    Load data from Parquet files to SQL Server.

    """
    # Establish connection
    cnxn = pyodbc.connect(
        'DRIVER=' + driver +
        ';SERVER=' + server + 
        ';PORT=1433;DATABASE=' + database + 
        ';UID=' + username + 
        ';PWD=' + password
    )
    cursor = cnxn.cursor()

    create_table_sql = f'''
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{table_name}')
    BEGIN
        CREATE TABLE {table_name} (
            [timestamp] DATE,
            [open] FLOAT,
            [high] FLOAT,
            [low] FLOAT,
            [close] FLOAT,
            [volume] INT,
            changes FLOAT
        )
    END
    '''

    # Execute the create table SQL statement
    cursor.execute(create_table_sql)
    cnxn.commit()

    folder_path = DATA_DIR

    parquet_files = [
        os.path.join(folder_path, file) 
        for file in os.listdir(folder_path) 
        if file.endswith('.parquet')
    ]

    # Read and concatenate Parquet files
    df = pd.concat(
        [pd.read_parquet(file) for file in parquet_files], 
        ignore_index=True
    )

    for index, row in df.iterrows():
        cursor.execute(
            (f"INSERT INTO {table_name} "
            "([timestamp], [open], [high], [low], [close], [volume], changes) VALUES (?, ?, ?, ?, ?, ?, ?)"), 
            row['timestamp'], row['open'], row['high'], row['low'], row['close'], row['volume'], row['changes']
        )
        cnxn.commit()

    # Close connection
    cursor.close()
    cnxn.close()


if __name__ == "__main__":
    # Load environment variables
    dotenv.load_dotenv()

    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_NAME')
    username = os.getenv('DB_USER')
    password = os.getenv('DB_PASS')
    driver= '{ODBC Driver 17 for SQL Server}'
    table_name = 'HPGstockprice'

    load_data(server, database, username, password, driver, table_name)