from pyspark.sql import DataFrame

class DriverUtil:

    @staticmethod
    def save_table_with_partition(df: DataFrame, table_name: str, partition_cols: list):
        df.write.partitionBy(*partition_cols) \
            .mode("overwrite") \
            .format("json") \
            .saveAsTable(table_name)
