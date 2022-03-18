import sqlite3
import dateparser
import pandas as pd
import plotly.express as px
from settings import DB_NAME, USAGE_TABLE_NAME

import warnings
warnings.filterwarnings('ignore')

def interpret_time(row):
    settings = dict(RELATIVE_BASE=row.timestamp.to_pydatetime())
    row.update_time = dateparser.parse(row.last_updated, settings=settings)
    return row

def match_updates(df):
    df['match'] = None
    for i, row in df.iterrows():
        if not df.match[i] and row['update_time']:
            matching = True
            next_ind = i + 1
            while matching and df.iloc[next_ind].update_time:
                delta = row['update_time'] - df.iloc[next_ind].update_time
                matching = abs(delta.total_seconds()) <= 600
                if matching:
                    df.iloc[next_ind].match = i
                next_ind += 1
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