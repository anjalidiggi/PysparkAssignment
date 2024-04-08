from pyspark.sql import DataFrame

class DriverUtil:

    @staticmethod
    def save_csv(df: DataFrame, path: str):
        df.write \
            .option("header", "true") \
            .option("sep", ",") \
            .mode("overwrite") \
            .csv(path)

    @staticmethod
    def save_table(df: DataFrame, table_name: str):
        df.write \
            .mode("overwrite") \
            .saveAsTable(table_name)
