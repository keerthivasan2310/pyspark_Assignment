import unittest
from pyspark.sql import SparkSession

from src.Question4.util import (
    convert_columns_snake_case,
    add_load_date,
    add_partition_columns
)


class TestEmployeeJSON(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[*]") \
            .appName("TestJSON") \
            .getOrCreate()

        data = [("0001", "JohnDoe")]
        cls.df = cls.spark.createDataFrame(data, ["id", "employeeName"])

    def test_snake_case(self):
        df2 = convert_columns_snake_case(self.df)
        self.assertIn("employee_name", df2.columns)

    def test_load_date(self):
        df2 = add_load_date(self.df)
        self.assertIn("load_date", df2.columns)

    def test_partition_columns(self):
        df2 = add_load_date(self.df)
        df3 = add_partition_columns(df2)
        self.assertTrue(
            all(col in df3.columns for col in ["year", "month", "day"])
        )


if __name__ == "__main__":
    unittest.main()
