from pyspark.sql.types import *
from pyspark.sql.functions import col, avg, lower, current_date


def create_df_dynamic(spark, data, columns):
    schema = StructType(
        [StructField(c, StringType(), True) for c in columns]
    )

    df = spark.createDataFrame(data, columns)

    for field in schema.fields:
        if field.name in ["employee_id", "salary", "Age"]:
            df = df.withColumn(field.name, col(field.name).cast("int"))

    return df


def avg_salary_by_department(employee_df):
    return employee_df.groupBy("department") \
        .agg(avg("salary").alias("avg_salary"))


def employees_name_start_m(employee_df, department_df):
    return employee_df.join(
        department_df,
        employee_df.department == department_df.dept_id,
        "inner"
    ).filter(lower(col("employee_name")).startswith("m")) \
     .select("employee_name", "dept_name")


def add_bonus(employee_df):
    return employee_df.withColumn(
        "bonus",
        col("salary") * 2
    )


def reorder_columns(employee_df):
    return employee_df.select(
        "employee_id",
        "employee_name",
        "salary",
        "State",
        "Age",
        "department"
    )


def dynamic_join(employee_df, department_df, join_type):
    return employee_df.join(
        department_df,
        employee_df.department == department_df.dept_id,
        join_type
    )


def replace_state_with_country(employee_df, country_df):
    return employee_df.join(
        country_df,
        employee_df.State == country_df.country_code,
        "left"
    ).drop("State", "country_code") \
     .withColumnRenamed("country_name", "State")


def lowercase_and_load_date(df):
    df = df.toDF(*[c.lower() for c in df.columns])
    return df.withColumn("load_date", current_date())


def write_external_tables(df):
    df.write.mode("overwrite") \
        .format("parquet") \
        .option("path", "external/parquet_employee") \
        .saveAsTable("employee.employee_parquet")

    df.write.mode("overwrite") \
        .format("csv") \
        .option("header", True) \
        .option("path", "external/csv_employee") \
        .saveAsTable("employee.employee_csv")
