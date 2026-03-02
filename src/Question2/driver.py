from pyspark.sql import SparkSession
from pyspark.sql.types import *

from util import (
    create_creditcard_df,
    get_partition_count,
    increase_partitions,
    decrease_partitions,
    add_masked_column
)

spark = SparkSession.builder.appName("Credit Card App").getOrCreate()

schema = StructType([
    StructField("card_number", StringType(), True)
])

data = [
    ("1234567891234567",),
    ("5678912345671234",),
    ("9123456712345678",),
    ("1234567812341122",),
    ("1234567812341342",)
]

credit_card_df = create_creditcard_df(spark, data, schema)

credit_card_df.show()

original_partitions = get_partition_count(credit_card_df)
print("Original partitions:", original_partitions)

increased_df = increase_partitions(credit_card_df, 5)
print("After increase:", get_partition_count(increased_df))

reduced_df = decrease_partitions(increased_df, original_partitions)
print("After decrease:", get_partition_count(reduced_df))

final_df = add_masked_column(reduced_df)

final_df.show(truncate=False)

spark.stop()
