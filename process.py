import sqlite3
import dateparser
import datetime as dt
import pandas as pd
import plotly.express as px
from settings import DB_NAME, USAGE_TABLE_NAME

import warnings
warnings.filterwarnings('ignore')

def interpret_time(row):
    """
    interpret_time estimates real update time based on timestamp and last_updated columns.

    :param row DataFrame row object.
    :return: 
    """
    if not isinstance(row.timestamp, dt.datetime):
        row.timestamp = pd.to_datetime(row.timestamp)
    settings = dict(RELATIVE_BASE=row.timestamp.to_pydatetime())
    row.update_time = dateparser.parse(row.last_updated, settings=settings)
    return row

def gen_update_time(df):
    """
    gen_update_time generates last updated column for scraped data.

    :param df DataFrame object.
    ;return: DataFrame with last updated column.
    """
    df.last_updated.replace(to_replace='Updated ', value='', regex=True, inplace=True)
    df.last_updated.replace(to_replace='moments', value='0 minutes', regex=True, inplace=True)
    df['update_time'] = None
    minutes_filter = df.last_updated.str.contains('minutes')
    df[minutes_filter] = df[minutes_filter].apply(interpret_time, axis=1)
    return df

def clean_data(db_name=DB_NAME, table_name=USAGE_TABLE_NAME):
    """
    clean_data removes duplicates and null values from usage DataFrame.

    :param db_name filename of sqlite3 database.
    :param table_name name of table with usage data.
    :returns cleaned DataFrame object.
    """
    con = sqlite3.connect(db_name)
    df = pd.read_sql(f"SELECT * FROM {table_name}", con)

    df = gen_update_time(df)
    df = df.dropna(subset=['update_time'])
    df = df.infer_objects()
    df = df.sort_values('update_time')

    df_new_ind = []
    for location in df.location.unique():
        df_loc = df[df.location == location].copy()
        rowA = df_loc.iloc[0]
        while rowA.name < df_loc.iloc[-1].name:
            df_new_ind.append(rowA.name)
            match_filter =  (df_loc.update_time - rowA.update_time).dt.total_seconds() > 600
            if len(df_loc[match_filter]) > 0:
                rowA = df_loc[match_filter].iloc[0]
            else:
                break
    return df.loc[df_new_ind]