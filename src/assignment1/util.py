from pyspark.sql.types import *
from pyspark.sql.functions import *


def create_purchase_data_df(spark):
    # Defining the schema for purchase_data_df
    purchase_schema = StructType([
        StructField("customer", IntegerType(), True),
        StructField("product_model", StringType(), True)
    ])

    # Create DataFrame for purchase_data_df
    purchase_data = [
        (1, "iphone13"),
        (1, "dell i5 core"),
        (2, "iphone13"),
        (2, "dell i5 core"),
        (3, "iphone13"),
        (3, "dell i5 core"),
        (1, "dell i3 core"),
        (1, "hp i5 core"),
        (1, "iphone14"),
        (3, "iphone14"),
        (4, "iphone13")
    ]

    return spark.createDataFrame(purchase_data, schema=purchase_schema)


def create_product_data_df(spark):
    # Defining the schema for product_data_df
    product_schema = StructType([
        StructField("product_model", StringType(), True)
    ])

    # Create DataFrame for product_data_df
    product_data = [
        ("iphone13",),
        ("dell i5 core",),
        ("dell i3 core",),
        ("hp i5 core",),
        ("iphone14",)
    ]

    return spark.createDataFrame(product_data, schema=product_schema)


# def find_customers_with_only_iphone13(purchase_data_df):
#     iphone13_customers = purchase_data_df.filter(purchase_data_df['product_model'] == 'iphone13')
#     iphone13_customers.show()
#     print("Successfully found customers who bought only iphone13.")


# only get the customer who purchased iphone 13 only
def find_customers_with_only_iphone13(purchase_data_df, product_data_df):
    filtered_purchase_df = purchase_data_df.groupBy("customer").count()
    count = filtered_purchase_df.filter("count = 1")
    res_df = count.join(purchase_data_df, "customer").join(product_data_df, "product_model").filter(
        "product_model = 'iphone13'").select("customer")
    return res_df


def find_customers_upgraded_to_iphone14(purchase_data_df):
    iphone13_customers = purchase_data_df.filter(purchase_data_df['product_model'] == 'iphone13')
    iphone14_customers = purchase_data_df.filter(purchase_data_df['product_model'] == 'iphone14')
    upgraded_customers = iphone13_customers.join(iphone14_customers, "customer", "inner")
    upgraded_customers_distinct = upgraded_customers.select("customer").distinct()
    return upgraded_customers_distinct


def find_customers_bought_all_products(purchase_data_df, product_data_df):
    unique_product_models = product_data_df.select("product_model").distinct()
    customer_product_count = purchase_data_df.groupBy("customer").agg(countDistinct("product_model").alias("product_count"))
    customers_bought_all_products = customer_product_count.filter(customer_product_count["product_count"] == unique_product_models.count()).select("customer")
    return customers_bought_all_products