import unittest
from pyspark.sql import SparkSession
from driver import main
from util import DriverUtil

class TestEmployeeAnalysisDataFrame(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestEmployeeAnalysisDataFrame") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_main(self):
        main()

if __name__ == "__main__":
    unittest.main(argv=[''], exit=False)

