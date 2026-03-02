from pyspark.sql import SparkSession
from util import (
    read_json_dynamic,
    flatten_df,
    record_counts,
    explode_examples,
    filter_id,
    convert_columns_snake_case,
    add_load_date,
    add_partition_columns,
    write_partitioned_table
)

spark = SparkSession.builder \
    .appName("Employee JSON Processing") \
    .enableHiveSupport() \
    .getOrCreate()

json_path = "input/employee.json"

df = read_json_dynamic(spark, json_path)

df.show()

flattened_df = flatten_df(df)

flattened_df.show()

original_count, flattened_count = record_counts(df, flattened_df)

print("Original Count:", original_count)
print("Flattened Count:", flattened_count)

explode_df, explode_outer_df, posexplode_df = explode_examples(df, "items")

explode_df.show()
explode_outer_df.show()
posexplode_df.show()

filtered_df = filter_id(df)

filtered_df.show()

snake_df = convert_columns_snake_case(flattened_df)

snake_df.show()

dated_df = add_load_date(snake_df)

partitioned_df = add_partition_columns(dated_df)

partitioned_df.show()

spark.sql("CREATE DATABASE IF NOT EXISTS employee")

write_partitioned_table(partitioned_df)

spark.stop()
