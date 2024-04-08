import unittest
from pyspark.sql import SparkSession
from driver import main
from util import DriverUtil

class TestJsonAnalysisDataFrame(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestJsonAnalysisDataFrame") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_main(self):
        main()

    def test_save_table_with_partition(self):
        df = self.spark.createDataFrame([(1, "A", "2023-09-05"), (2, "B", "2023-09-06")], ["id", "name", "date"])
        DriverUtil.save_table_with_partition(df, "test_table", ["date"])

if __name__ == "__main__":
    unittest.main(argv=[''], exit=False)

