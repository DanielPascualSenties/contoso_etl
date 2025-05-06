from myFunctions import datahub_format

import unittest
from pyspark.sql import Row

class TestDatahubFormat(unittest.TestCase):

    def setUp(self):
        self.df = spark.createDataFrame([Row(name='Alice', age=30), Row(name='Bob', age=25)])

    def test_column_names_uppercase(self):
        result_df = datahub_format(self.df)
        expected_columns = ['NAME', 'AGE']
        self.assertEqual(result_df.columns, expected_columns)

if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)