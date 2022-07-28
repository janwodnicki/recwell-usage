import sqlite3
import pandas as pd
from settings import DB_NAME, USAGE_TABLE_NAME

con = sqlite3.connect(DB_NAME)
print(pd.read_sql(f'SELECT * FROM {USAGE_TABLE_NAME} GROUP BY location HAVING MAX(update_time)', con))
