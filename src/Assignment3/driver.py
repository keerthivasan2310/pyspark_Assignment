from pyspark.sql import SparkSession
from util import (
    create_log_df,
    rename_columns_dynamic,
    actions_last_7_days,
    convert_to_login_date,
    write_csv,
    write_managed_table
)

spark = SparkSession.builder \
    .appName("User Activity") \
    .enableHiveSupport() \
    .getOrCreate()

data = [
    (1, 101, 'login', '2023-09-05 08:30:00'),
    (2, 102, 'click', '2023-09-06 12:45:00'),
    (3, 101, 'click', '2023-09-07 14:15:00'),
    (4, 103, 'login', '2023-09-08 09:00:00'),
    (5, 102, 'logout', '2023-09-09 17:30:00'),
    (6, 101, 'click', '2023-09-10 11:20:00'),
    (7, 103, 'click', '2023-09-11 10:15:00'),
    (8, 102, 'click', '2023-09-12 13:10:00')
]

df = create_log_df(spark, data)

new_columns = ["log_id", "user_id", "user_activity", "time_stamp"]
df = rename_columns_dynamic(df, new_columns)

df.show()

last7_df = actions_last_7_days(df)
last7_df.show()

df = convert_to_login_date(df)
df.show()

write_csv(df, "output/user_activity_csv")

spark.sql("CREATE DATABASE IF NOT EXISTS user")

write_managed_table(df)

spark.stop()
