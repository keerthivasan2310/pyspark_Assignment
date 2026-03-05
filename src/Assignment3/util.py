from pyspark.sql.types import *
from pyspark.sql.functions import col, to_timestamp, to_date, date_sub, current_date


def create_log_df(spark, data):
    schema = StructType([
        StructField("log_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("user_activity", StringType(), True),
        StructField("time_stamp", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


def rename_columns_dynamic(df, new_columns):
    return df.toDF(*new_columns)


def actions_last_7_days(df):
    df = df.withColumn(
        "time_stamp",
        to_timestamp(col("time_stamp"))
    )

    filtered = df.filter(
        col("time_stamp") >= date_sub(current_date(), 7)
    )

    return filtered.groupBy("user_id").count()


def convert_to_login_date(df):
    df = df.withColumn(
        "time_stamp",
        to_timestamp(col("time_stamp"))
    )

    return df.withColumn(
        "login_date",
        to_date(col("time_stamp"))
    )


def write_csv(df, path):
    df.write \
        .mode("overwrite") \
        .option("header", True) \
        .option("delimiter", ",") \
        .option("nullValue", "NA") \
        .csv(path)


def write_managed_table(df):
    df.write \
        .mode("overwrite") \
        .saveAsTable("user.login_details")
