import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from src.Question1.util import (
    customers_only_iphone13,
    customers_upgraded,
    customers_all_products
)


class TestUtil(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]") \
            .appName("Test").getOrCreate()

        purchase_data = [
            (1, "iphone13"),
            (1, "iphone14"),
            (4, "iphone13")
        ]

        product_data = [
            ("iphone13",),
            ("iphone14",)
        ]

        schema1 = StructType([
            StructField("customer", IntegerType(), True),
            StructField("product", StringType(), True)
        ])

        schema2 = StructType([
            StructField("product", StringType(), True)
        ])

        cls.purchase_df = cls.spark.createDataFrame(purchase_data, schema1)
        cls.product_df = cls.spark.createDataFrame(product_data, schema2)

    def test_only_iphone13(self):
        result = customers_only_iphone13(self.purchase_df)
        self.assertEqual(result.count(), 1)

    def test_upgrade(self):
        result = customers_upgraded(self.purchase_df)
        self.assertEqual(result.count(), 1)

    def test_all_products(self):
        result = customers_all_products(
            self.purchase_df,
            self.product_df
        )
        self.assertEqual(result.count(), 1)


if __name__ == "__main__":
    unittest.main()
