import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from src.Question2.util import (
    create_creditcard_df,
    get_partition_count,
    increase_partitions,
    decrease_partitions,
    add_masked_column
)


class TestCreditCardUtil(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[*]") \
            .appName("TestCreditCard") \
            .getOrCreate()

        cls.schema = StructType([
            StructField("card_number", StringType(), True)
        ])

        cls.data = [
            ("1234567891234567",),
            ("5678912345671234",)
        ]

        cls.df = create_creditcard_df(
            cls.spark,
            cls.data,
            cls.schema
        )

    def test_partition_count(self):
        self.assertTrue(get_partition_count(self.df) >= 1)

    def test_increase_partition(self):
        df2 = increase_partitions(self.df, 5)
        self.assertEqual(get_partition_count(df2), 5)

    def test_mask_column(self):
        masked_df = add_masked_column(self.df)
        self.assertIn("masked_card_number", masked_df.columns)

    def test_decrease_partition(self):
        df2 = increase_partitions(self.df, 5)
        df3 = decrease_partitions(df2, 1)
        self.assertEqual(get_partition_count(df3), 1)


if __name__ == "__main__":
    unittest.main()
