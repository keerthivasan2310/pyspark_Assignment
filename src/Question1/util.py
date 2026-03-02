from pyspark.sql.functions import col, collect_list, collect_set, size



# Customers who bought only iphone13

def customers_only_iphone13(purchase_df):

    result = purchase_df.groupBy("customer") \
        .agg(collect_list("product").alias("pro")) \
        .filter((size("pro") == 1) & (col("pro")[0] == "iphone13")) \
        .select("customer")

    return result


# Customers upgraded iphone13 -> iphone14

def customers_upgraded(purchase_df):

    ip13 = purchase_df.filter(
        col("product") == "iphone13"
    ).select("customer")

    ip14 = purchase_df.filter(
        col("product") == "iphone14"
    ).select("customer")

    return ip13.intersect(ip14)



# Customers bought all products

def customers_all_products(purchase_df, product_df):

    total_products = product_df.count()

    result = purchase_df.groupBy("customer") \
        .agg(collect_set("product").alias("pro")) \
        .filter(size("pro") == total_products) \
        .select("customer")

    return result
