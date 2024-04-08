from pyspark.sql import SparkSession
from util import DriverUtil

def main():
    spark = SparkSession.builder \
        .appName("JSON Analysis DataFrame") \
        .getOrCreate()

    # Read JSON file with dynamic schema
    df = spark.read.option("multiline", "true").json("data.json")

    # Flatten the DataFrame
    flat_df = df.selectExpr("id", "name", "explode(options) as options", "load_date")

    # Find record count when flattened and not flattened
    record_count_flattened = flat_df.count()
    record_count_original = df.count()
    record_count_difference = record_count_flattened - record_count_original

    # Differentiate using explode, explode_outer, posexplode functions
    exploded_df = flat_df.withColumn("options", col("options").getItem("option").alias("options"))
    exploded_outer_df = flat_df.withColumn("options", col("options").getItem("option").alias("options"))
    pos_exploded_df = flat_df.withColumn("options", col("options").getItem("option").alias("options"))

    # Filter the id equal to 0001
    filtered_df = flat_df.filter(col("id") == "0001")

    # Convert column names from camel case to snake case
    snake_case_df = flat_df.toDF(*(col_name.lower() for col_name in flat_df.columns))

    # Add a new column named load_date with the current date
    load_date_df = flat_df.withColumn("load_date", to_date(col("load_date"), "yyyy-MM-dd"))

    # Create year, month, day columns from load_date
    date_columns_df = load_date_df.withColumn("year", year(col("load_date"))) \
                                  .withColumn("month", month(col("load_date"))) \
                                  .withColumn("day", dayofmonth(col("load_date")))

    # Write DataFrame to a table with partition
    DriverUtil.save_table_with_partition(date_columns_df, "employee.employee_details", ["year", "month", "day"])

    spark.stop()

if __name__ == "__main__":
    main()
