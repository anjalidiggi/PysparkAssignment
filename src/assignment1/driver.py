from pyspark.sql import SparkSession
from src.Assignment1.util import *


def main():
    # Initialize SparkSession
    spark = SparkSession.builder.appName("Purchase Analysis").getOrCreate()

    # Create DataFrame for purchase data
    purchase_data_df = create_purchase_data_df(spark)

    # Create DataFrame for product data
    product_data_df = create_product_data_df(spark)

    # Find customers who have bought only iphone13
    print("Customers who bought only iphone13:")
    result_df=find_customers_with_only_iphone13(purchase_data_df,product_data_df)
    result_df.show()

    # Find customers who upgraded from iphone13 to iphone14
    print("\nCustomers who upgraded from iphone13 to iphone14:")
    result_df = find_customers_upgraded_to_iphone14(purchase_data_df)
    result_df.show()

    # Find customers who have bought all products
    print("\nCustomers who bought all products:")
    result_df = find_customers_bought_all_products(purchase_data_df, product_data_df)
    result_df.show()

    spark.stop()


if __name__ == "__main__":
    main()