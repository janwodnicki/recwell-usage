import sqlite3
import time
import pandas as pd
import datetime as dt

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from settings import DB_NAME, USAGE_TABLE_NAME, LIVE_USAGE_URL

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

def save_updates(df):
    """
    save_updates saves DataFrame to SQLite database.

    :param df: DataFrame object.
    """
    dtypes = dict(
        current_count='Integer',
        max_count='Integer'
    )
    con = sqlite3.connect(DB_NAME)
    df.to_sql(USAGE_TABLE_NAME, con, if_exists='append', index=False, dtype=dtypes)
    con.close()

def main():
    # Initialize webdriver
    firefox_options = Options()
    firefox_options.add_argument('--headless')
    driver = webdriver.Firefox(options=firefox_options)

    # Extract data from live-tracker elements and create df
    trackers = get_trackers(driver)
    data = [tracker_details(t) for t in trackers]
    df = pd.DataFrame(data)
    df['timestamp'] = dt.datetime.now()
    save_updates(df)
    driver.close()
    
if __name__ == '__main__':
    main()