import unittest
from pyspark.sql import SparkSession
from src.Question5.util import (
    create_df_dynamic,
    add_bonus,
    lowercase_and_load_date
)


class TestEmployeeProcessing(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[*]") \
            .appName("TestEmployee") \
            .getOrCreate()

        data = [(1,"mike","D101","ny",1000,25)]
        cols = ["employee_id","employee_name","department","State","salary","Age"]

        cls.df = create_df_dynamic(cls.spark, data, cols)

    def test_bonus(self):
        df2 = add_bonus(self.df)
        self.assertIn("bonus", df2.columns)

    def test_lowercase(self):
        df2 = lowercase_and_load_date(self.df)
        self.assertIn("load_date", df2.columns)


if __name__ == "__main__":
    unittest.main()
