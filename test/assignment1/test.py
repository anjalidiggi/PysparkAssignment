import unittest
from pyspark.sql import SparkSession
from src.Assignment1.util import *


class TestPurchaseAnalysis(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestPurchaseAnalysis").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_only_iphone13_customers(self):
        purchase_data_df = create_purchase_data_df(self.spark)
        product_data_df = create_product_data_df(self.spark)
        result = find_customers_with_only_iphone13(purchase_data_df, product_data_df)
        expected_result = [(4,)]  # Change the format to match the actual result
        result_list = [(row.customer,) for row in result.collect()]  # Convert Row objects to tuples
        self.assertEqual(result_list, expected_result)

    def test_customers_upgraded_to_iphone14(self):
        purchase_data_df = create_purchase_data_df(self.spark)
        result_df = find_customers_upgraded_to_iphone14(purchase_data_df)
        expected_result = [(1,), (3,)]  # Adjust the expected result based on your data
        result_list = [(row.customer,) for row in result_df.collect()]
        self.assertEqual(sorted(result_list), sorted(expected_result))

    def test_customers_bought_all_products(self):
        purchase_data_df = create_purchase_data_df(self.spark)
        product_data_df = create_product_data_df(self.spark)
        result = find_customers_bought_all_products(purchase_data_df, product_data_df)
        expected_result = [(1,)]
        self.assertEqual(result.collect(), expected_result)


if __name__ == "__main__":
    unittest.main()