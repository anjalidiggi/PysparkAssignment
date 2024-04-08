from pyspark.sql import SparkSession
from util import DriverUtil

def main():
    spark = SparkSession.builder \
        .appName("Custom Schema DataFrame") \
        .getOrCreate()

    # Create DataFrame
    data = [
        (1, 101, 'login', '2023-09-05 08:30:00'),
        (2, 102, 'click', '2023-09-06 12:45:00'),
        (3, 101, 'click', '2023-09-07 14:15:00'),
        (4, 103, 'login', '2023-09-08 09:00:00'),
        (5, 102, 'logout', '2023-09-09 17:30:00'),
        (6, 101, 'click', '2023-09-10 11:20:00'),
        (7, 103, 'click', '2023-09-11 10:15:00'),
        (8, 102, 'click', '2023-09-12 13:10:00')
    ]
    schema = StructType([
        StructField("log_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])
    df = spark.createDataFrame(data, schema)

    # Rename columns using dynamic function
    df = df.withColumnRenamed("log_id", "log_id") \
        .withColumnRenamed("user_id", "user_id") \
        .withColumnRenamed("action", "user_activity") \
        .withColumnRenamed("timestamp", "time_stamp")

    # Convert time_stamp column to login_date column with date format
    df = df.withColumn("login_date", date_format(col("time_stamp"), "yyyy-MM-dd").cast("date"))

    # Calculate the number of actions performed by each user in the last 7 days
    from pyspark.sql.window import Window
    from pyspark.sql import functions as F
    windowSpec = Window.partitionBy("user_id").orderBy("login_date").rangeBetween(-6, 0)
    df = df.withColumn("actions_last_7_days", F.count("user_id").over(windowSpec))

    # Write DataFrame as CSV file with different write options
    DriverUtil.save_csv(df, "output")

    # Write DataFrame as a managed table
    DriverUtil.save_table(df, "user.login_details")

    spark.stop()

if __name__ == "__main__":
    main()
