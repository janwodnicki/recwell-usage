from cgi import test
import unittest
import pandas as pd

from scrape import *

class TestInterpretTime(unittest.TestCase):

    df1 = pd.DataFrame({
        'location': 'Nick Level 1 Fitness',
        'last_updated': 'Updated 38 minutes ago',
        'current_count': '31',
        'max_count': '45',
        'timestamp': '2022-07-25 17:22:36.171643'
    }, index=[0])
    df1.timestamp = pd.to_datetime(df1.timestamp)

    df2 = pd.DataFrame({
        'location': 'Nick Level 1 Fitness',
        'last_updated': 'Updated 53 minutes ago',
        'current_count': '37',
        'max_count': '45',
        'timestamp': '2022-07-25 17:37:09.489139'
    }, index=[0])
    df2.timestamp = pd.to_datetime(df2.timestamp)

    # Test 1: Check whether df1 = df2
    def test_interpret_time(self):
        df1_interpreted = gen_update_time(self.df1)
        print(df1_interpreted)

if __name__ == "__main__":
    unittest.main()
