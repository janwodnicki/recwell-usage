import sqlite3
import dateparser
import pandas as pd
import plotly.express as px
from settings import DB_NAME, USAGE_TABLE_NAME

import warnings
warnings.filterwarnings("ignore")

def last_updated_to_datetime(row):
    timestamp_datetime = dateparser.parse(row.timestamp)
    settings = dict(RELATIVE_BASE=timestamp_datetime)
    last_updated_clean = row.last_updated.replace('Updated ', '').replace('moments', '0 minutes')

    # Ignore updates older than an hour for simplicity, will be removed later
    if 'minutes' in last_updated_clean:
        update_time = dateparser.parse(last_updated_clean, settings=settings)
    else:
        update_time = None

    row.last_updated = last_updated_clean
    row['update_time'] = update_time
    return row

def match_updates(df):
    df['match'] = None
    for i, row in df.iterrows():
        if not df.match[i] and row['update_time']:
            deltas = (row['update_time'] - df.update_time)
            delta_filter = deltas.dt.total_seconds().abs().le(600)
            df.match[delta_filter] = i
    return df

def reduce_updates(df):
    reduced = df[df.last_updated.str.contains('minutes')]
    return reduced.drop_duplicates(subset=['location', 'current_count', 'match'], keep='first')

def process_updates(db_name=DB_NAME, table_name=USAGE_TABLE_NAME):
    conn = sqlite3.connect(db_name)
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    df_update_time = df.apply(last_updated_to_datetime, axis=1)
    matched = match_updates(df_update_time)
    return reduce_updates(matched)

if __name__ == '__main__':
    process_updates()