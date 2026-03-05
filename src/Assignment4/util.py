import re
from pyspark.sql.functions import (
    col, explode, explode_outer, posexplode,
    current_date, year, month, dayofmonth
)


def read_json_dynamic(spark, path):
    return spark.read.option("multiline", True).json(path)


def flatten_df(df):
    cols = []
    for field in df.schema.fields:
        if field.dataType.typeName() == "struct":
            for nested in field.dataType.fields:
                cols.append(col(f"{field.name}.{nested.name}")
                            .alias(f"{field.name}_{nested.name}"))
        else:
            cols.append(col(field.name))
    return df.select(cols)


def record_counts(original_df, flattened_df):
    return original_df.count(), flattened_df.count()


def explode_examples(df, column_name):
    exp = df.select(explode(col(column_name)).alias("explode_col"))
    exp_outer = df.select(explode_outer(col(column_name)).alias("explode_outer_col"))
    pos_exp = df.select(posexplode(col(column_name)).alias("pos", "posexplode_col"))
    return exp, exp_outer, pos_exp


def filter_id(df):
    return df.filter(col("id") == "0001")


def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def convert_columns_snake_case(df):
    new_cols = [camel_to_snake(c) for c in df.columns]
    return df.toDF(*new_cols)


def add_load_date(df):
    return df.withColumn("load_date", current_date())


def add_partition_columns(df):
    return df.withColumn("year", year(col("load_date"))) \
             .withColumn("month", month(col("load_date"))) \
             .withColumn("day", dayofmonth(col("load_date")))


def write_partitioned_table(df):
    df.write \
        .mode("overwrite") \
        .format("json") \
        .partitionBy("year", "month", "day") \
        .option("replaceWhere", "year IS NOT NULL") \
        .saveAsTable("employee.employee_details")
