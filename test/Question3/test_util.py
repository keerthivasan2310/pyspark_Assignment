import unittest
from pyspark.sql import SparkSession

from src.Question3.util import (
    create_log_df,
    rename_columns_dynamic,
    actions_last_7_days,
    convert_to_login_date
)


class TestUserActivity(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[*]") \
            .appName("TestUserActivity") \
            .getOrCreate()

        cls.data = [
            (1, 101, 'login', '2023-09-10 08:30:00'),
            (2, 101, 'click', '2023-09-11 09:00:00')
        ]

        cls.df = create_log_df(cls.spark, cls.data)

    def test_dynamic_columns(self):
        cols = ["log_id", "user_id", "user_activity", "time_stamp"]
        df2 = rename_columns_dynamic(self.df, cols)
        self.assertEqual(df2.columns, cols)

    def test_login_date_conversion(self):
        df2 = convert_to_login_date(self.df)
        self.assertIn("login_date", df2.columns)

    def test_last7days(self):
        result = actions_last_7_days(self.df)
        self.assertTrue(result.count() >= 1)


if __name__ == "__main__":
    unittest.main()
