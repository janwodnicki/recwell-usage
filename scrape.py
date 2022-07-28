import sqlite3
import time
import pandas as pd
import datetime as dt

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By

from settings import DB_NAME, USAGE_TABLE_NAME, LIVE_USAGE_URL
from process import gen_update_time

import warnings
warnings.filterwarnings('ignore')

def get_trackers(driver):
    """
    get_trackers finds RecWell live usage elements.

    :param driver: webdriver object.
    :return: list of webdriver elements.
    """
    driver.get(LIVE_USAGE_URL)
    time.sleep(3)
    return driver.find_elements(By.CLASS_NAME, 'live-tracker')

def tracker_details(tracker):
    """
    tracker_details extracts text from webdriver element corresponding to RecWell live usage.

    :param tracker: webdriver element for live-tracker div element.
    :return: dict containing location, last updated, current count, and max count strings.
    """
    class_col_pairs = [
        ('tracker-location', 'location'),
        ('tracker-update-time', 'last_updated'),
        ('tracker-current-count', 'current_count'),
        ('tracker-max-count', 'max_count')
    ]
    return {col: tracker.find_element(By.CLASS_NAME, class_).text for class_, col in class_col_pairs}

def check_unique(rowA, rowB) -> bool:
    """
    check_unique determines whether latest data is new.

    :param row1 Series object.
    :param row2 Series object.
    :return: bool whether new data is unique.
    """
    try:
        delta = pd.to_datetime(rowA.update_time) - pd.to_datetime(rowB.update_time)
        return abs(delta.total_seconds()) >= 600
    except Exception as e:
        return False

def find_unique(df, db_name=DB_NAME, usage_table=USAGE_TABLE_NAME):
    con = sqlite3.connect(db_name)
    df_old = pd.read_sql(f"SELECT * FROM {usage_table} GROUP BY location HAVING MAX(timestamp)", con)
    df_new = pd.DataFrame([], columns = df.columns)
    for location in df.location.unique():
        rowA = df[df.location.str.contains(location)].iloc[0]
        if not rowA.update_time:
            continue
        elif df_old.location.isin([location]).any():
            rowB = df_old[df_old.location.str.contains(location)].iloc[0]
            if not rowB.update_time or not check_unique(rowA, rowB):
                continue
        df_new = df_new.append(rowA)
    return df_new

def save_updates(df, db_name=DB_NAME, usage_table=USAGE_TABLE_NAME):
    """
    save_updates saves DataFrame to SQLite database.

    :param df: DataFrame object.
    """
    dtypes = dict(
        current_count='Integer',
        max_count='Integer'
    )
    con = sqlite3.connect(db_name)
    df.to_sql(usage_table, con, if_exists='append', index=False, dtype=dtypes)
    con.close()

def get_raw_data():
    # Initialize webdriver
    firefox_options = Options()
    firefox_options.add_argument('--headless')
    driver = webdriver.Firefox(options=firefox_options)

    # Extract data from live-tracker elements and create df
    trackers = get_trackers(driver)
    data = [tracker_details(t) for t in trackers]
    driver.close()
    df = pd.DataFrame(data)
    df['timestamp'] = dt.datetime.now()
    return df

def main(db_name=DB_NAME, usage_table=USAGE_TABLE_NAME, test=False):
    df = get_raw_data()
    df = gen_update_time(df)
    df = find_unique(df, db_name, usage_table)
    if test:
        return df
    save_updates(df, db_name, usage_table)
    
if __name__ == '__main__':
    main()