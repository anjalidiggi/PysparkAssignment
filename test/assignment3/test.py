import unittest
from pyspark.sql import SparkSession
from driver import main
from util import DriverUtil

class TestCustomSchemaDataFrame(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestCustomSchemaDataFrame") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_main(self):
        main()

    def test_save_csv(self):
        df = self.spark.createDataFrame([(1, "A"), (2, "B")], ["id", "name"])
        DriverUtil.save_csv(df, "output.csv")

    def test_save_table(self):
        df = self.spark.createDataFrame([(1, "A"), (2, "B")], ["id", "name"])
        DriverUtil.save_table(df, "user.test_table")

if __name__ == "__main__":
    unittest.main(argv=[''], exit=False)

