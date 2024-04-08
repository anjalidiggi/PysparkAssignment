from pyspark.sql import DataFrame

class DriverUtil:

    @staticmethod
    def save_table_with_partition(df: DataFrame, table_name: str, partition_cols: list):
        df.write.partitionBy(*partition_cols) \
            .mode("overwrite") \
            .format("parquet") \
            .saveAsTable(table_name)

    @staticmethod
    def save_table_as_csv(df: DataFrame, table_name: str):
        df.write.mode("overwrite") \
            .format("csv") \
            .saveAsTable(table_name)
