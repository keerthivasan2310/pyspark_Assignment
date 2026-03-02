from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, col


def create_creditcard_df(spark, data, schema):
    df = spark.createDataFrame(data, schema)

    spark.sparkContext.parallelize(data).toDF(["card_number"])

    from pyspark.sql import Row
    spark.createDataFrame([Row(card_number=x[0]) for x in data])

    return df


def get_partition_count(df):
    return df.rdd.getNumPartitions()


def increase_partitions(df, num):
    return df.repartition(num)


def decrease_partitions(df, original_num):
    return df.coalesce(original_num)


def mask_card_number():
    def mask(card):
        if card is None:
            return None
        return "*" * (len(card) - 4) + card[-4:]

    return udf(mask, StringType())


def add_masked_column(df):
    mask_udf = mask_card_number()

    return df.withColumn(
        "masked_card_number",
        mask_udf(col("card_number"))
    )
