from pyspark.sql import SparkSession
from util import *

spark = SparkSession.builder \
    .appName("Employee Processing") \
    .enableHiveSupport() \
    .getOrCreate()

employee_data = [
    (11,"james","D101","ny",9000,34),
    (12,"michel","D101","ny",8900,32),
    (13,"robert","D102","ca",7900,29),
    (14,"scott","D103","ca",8000,36),
    (15,"jen","D102","ny",9500,38),
    (16,"jeff","D103","uk",9100,35),
    (17,"maria","D101","ny",7900,40)
]

department_data = [
    ("D101","sales"),
    ("D102","finance"),
    ("D103","marketing"),
    ("D104","hr"),
    ("D105","support")
]

country_data = [
    ("ny","newyork"),
    ("ca","California"),
    ("uk","Russia")
]

employee_cols = ["employee_id","employee_name","department","State","salary","Age"]
dept_cols = ["dept_id","dept_name"]
country_cols = ["country_code","country_name"]

employee_df = create_df_dynamic(spark, employee_data, employee_cols)
department_df = create_df_dynamic(spark, department_data, dept_cols)
country_df = create_df_dynamic(spark, country_data, country_cols)

avg_salary_by_department(employee_df).show()

employees_name_start_m(employee_df, department_df).show()

employee_df = add_bonus(employee_df)

employee_df = reorder_columns(employee_df)

dynamic_join(employee_df, department_df, "inner").show()
dynamic_join(employee_df, department_df, "left").show()
dynamic_join(employee_df, department_df, "right").show()

country_employee_df = replace_state_with_country(employee_df, country_df)

country_employee_df.show()

final_df = lowercase_and_load_date(country_employee_df)

final_df.show()

spark.sql("CREATE DATABASE IF NOT EXISTS employee")

write_external_tables(final_df)

spark.stop()
