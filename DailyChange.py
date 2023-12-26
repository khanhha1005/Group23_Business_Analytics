import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


df = pd.read_csv('HPG_day_ohlc.csv')
def parse_datetime(time_str):
    if '-' in time_str:
        # Format: 2023-01-06T02:00:00
        return pd.to_datetime(time_str, format='%Y-%m-%dT%H:%M:%S')
    else:
        # Format: 12/23/2015
        return pd.to_datetime(time_str, format='%m/%d/%Y')

df['Time'] = df['Time'].apply(parse_datetime)

# Extract year, month, and day
df['Year'] = df['Time'].dt.year
df['Month'] = df['Time'].dt.month
df['Day'] = df['Time'].dt.day

df["Changes"] = round((df["Close"] - df["Open"]) / df["Open"] * 100, 2)
df = df.sort_values(by=["Year", "Month", "Day"], ascending=[True, True, True])


df.to_csv("HPG.csv")


