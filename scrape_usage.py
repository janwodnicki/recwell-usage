import sqlite3
import time
import pandas as pd
import datetime as dt

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from settings import DF_NAME, USAGE_TABLE_NAME, LIVE_USAGE_URL

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

def main():
    # Initialize webdriver
    firefox_options = Options()
    firefox_options.add_argument('--headless')
    driver = webdriver.Firefox(options=firefox_options)

    # Find live-tracker elements
    driver.get(LIVE_USAGE_URL)
    time.sleep(3)
    trackers = driver.find_elements(By.CLASS_NAME, 'live-tracker')

    # Extract data from live-tracker elements and create df
    trackers_details = [tracker_details(tracker) for tracker in trackers]
    df = pd.DataFrame(trackers_details)
    df['timestamp'] = dt.datetime.now()

    # Append dataframe to database table
    conn = sqlite3.connect(DF_NAME)
    df.to_sql(USAGE_TABLE_NAME, conn, if_exists='append', index=False)

    # Close connections
    conn.close()
    driver.close()
    
if __name__ == '__main__':
    main()