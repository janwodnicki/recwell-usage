import sqlite3
import pandas as pd
from settings import DB_NAME, USAGE_TABLE_NAME
from process import process_updates

con = sqlite3.connect(DB_NAME)
df_data = pd.read_sql(f'SELECT * FROM {USAGE_TABLE_NAME} ORDER BY timestamp DESC', con)
timestamps = pd.to_datetime(df_data.timestamp)
days = (timestamps.iloc[0] - timestamps.iloc[-1]).days + 1
print(f'Last updated: {df_data.timestamp.iloc[0]}')
print(f'Database size: {len(df_data)} row(s), {days} day(s) \n')

df = process_updates()
print(df.drop_duplicates(subset=['location'], keep='last'))