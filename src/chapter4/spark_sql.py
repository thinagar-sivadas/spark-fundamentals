"""Chapter 4 - spark_sql.py"""

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def create_schema():
    """This function returns the schema for the dataframe"""

    schema = StructType(
        [
            StructField("date", StringType(), True),
            StructField("delay", IntegerType(), True),
            StructField("distance", IntegerType(), True),
            StructField("origin", StringType(), True),
            StructField("destination", StringType(), True),
        ]
    )

    return schema


if __name__ == "__main__":
    spark = SparkSession.builder.appName("spark_sql").getOrCreate()
    df = (
        spark.read.format("csv")
        .schema(create_schema())
        .option("header", "true")
        .load("/opt/bitnami/spark/custom_data/chapter4/departuredelays.csv")
    )
    print(df.show())

    # create a temporary view
    spark.catalog.dropTempView("us_delay_flights_tbl")
    df.createOrReplaceTempView("us_delay_flights_tbl")

    # run SQL query using Spark SQL
    print(
        spark.sql(
            """
            SELECT distance, origin, destination
            FROM us_delay_flights_tbl WHERE distance > 1000
            ORDER BY distance DESC
            """
        ).show(10)
    )

    print(
        spark.sql(
            """
            SELECT date, distance, origin, destination
            FROM us_delay_flights_tbl WHERE delay > 120 AND
            origin = 'SFO' and destination='ORD'
            ORDER BY delay DESC
            """
        ).show(10)
    )

    print(
        spark.sql(
            """
            SELECT delay, origin, destination,
            CASE
                WHEN delay>360 THEN 'Very Long Delays'
                WHEN delay>=120 and delay<=360 THEN 'Long Delays'
                WHEN delay>=60 and delay <120 THEN 'Short Delays'
                WHEN delay>0 and delay<60 THEN  'Tolerable Delays'
                WHEN delay=0 THEN 'No Delays'
            END as Flight_Delays
            FROM us_delay_flights_tbl
            ORDER BY origin, delay DESC
            """
        ).show(10)
    )

    # Create database and table
    spark.sql("""CREATE DATABASE IF NOT EXISTS learn_spark_db""")
    spark.sql("""use learn_spark_db""")

    # Manage table
    spark.sql("""DROP TABLE IF EXISTS managed_us_delay_flights_tbl""")
    df.write.mode("overwrite").saveAsTable("managed_us_delay_flights_tbl")

    # Unmanaged table
    spark.sql("""DROP TABLE IF EXISTS unmanaged_us_delay_flights_tbl""")
    df.write.option("path", "/opt/bitnami/spark/custom_data/chapter4/output/").mode("overwrite").saveAsTable(
        "unmanaged_us_delay_flights_tbl"
    )

    # View metadata
    print(spark.catalog.listDatabases())
    print(spark.catalog.listTables())
    print(spark.catalog.listColumns("unmanaged_us_delay_flights_tbl"))
    df_table = spark.table("us_delay_flights_tbl")
    print(df_table.printSchema())

    spark.stop()
