"""Chapter 5 - external_datasources_common_operations.py"""

import psycopg2
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Window

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("external_datasources_common_operations")
        .config("spark.jars", "/opt/bitnami/spark/connectors/jars/postgresql-42.6.0.jar")
        .config("spark.driver.extraClassPath", "/opt/bitnami/spark/connectors/jars/postgresql-42.6.0.jar")
        .getOrCreate()
    )

    connection_params = {
        "host": "host.docker.internal",
        "port": 5432,
        "database": "postgres",
        "user": "postgres",
        "password": "postgres",
    }

    # Drop table if exists in postgres
    table_names = ["airport", "delay"]
    with psycopg2.connect(**connection_params) as conn, conn.cursor() as cursor:
        for table in table_names:
            sql_query = f"DROP TABLE IF EXISTS {table}"
            cursor.execute(sql_query)

    # Create table in postgres
    with psycopg2.connect(**connection_params) as conn, conn.cursor() as cursor:
        cursor.execute(
            """CREATE TABLE airport (
                            "City" TEXT,
                            "State" TEXT,
                            "Country" TEXT,
                            "IATA" TEXT
                            )"""
        )
        cursor.execute(
            """CREATE TABLE delay (
                            date TEXT,
                            delay INT,
                            distance INT,
                            origin TEXT,
                            destination TEXT
                            )"""
        )

    # Load data into postgres
    airport = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", "\t")
        .load("/opt/bitnami/spark/custom_data/chapter5/airport-codes-na.txt")
    )
    delay = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/opt/bitnami/spark/custom_data/chapter5/departuredelays.csv")
    )

    airport.write.format("jdbc").option("url", "jdbc:postgresql://host.docker.internal:5432/postgres").option(
        "dbtable", "airport"
    ).option("user", "postgres").option("password", "postgres").mode("append").save()

    delay.write.format("jdbc").option("url", "jdbc:postgresql://host.docker.internal:5432/postgres").option(
        "dbtable", "delay"
    ).option("user", "postgres").option("password", "postgres").mode("append").save()

    # Read data from postgres
    airport = (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://host.docker.internal:5432/postgres")
        .option("dbtable", "airport")
        .option("user", "postgres")
        .option("password", "postgres")
        .load()
    )

    delay = (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://host.docker.internal:5432/postgres")
        .option("dbtable", "delay")
        .option("user", "postgres")
        .option("password", "postgres")
        .load()
    )
    # .option('numPartitions', 10)  # Performs query in parallel, start with multiple of the number of spark workers.
    # This option can work alone without the below options. The jdbc will determine the partitions automatically.
    # .option('partitionColumn', 'your_partition_column')  # Specify the column to partition on
    # .option('lowerBound', 1)  # Specify the lower bound value for the partitioning
    # .option('upperBound', 100)  # Specify the upper bound value for the partitioning
    # .option('query', "SELECT * FROM delay limit 10") # To perform custom sql to extract data

    # Common operations
    airport.createOrReplaceTempView("airports")
    delay.createOrReplaceTempView("departureDelays")

    delay_filter = delay.filter(F.expr("""origin=='SEA' and destination=='SFO' and delay>0 and date like '1220%'"""))

    # Union
    delay_union = delay.union(delay_filter)
    print(
        delay_union.filter(F.expr("""origin=='SEA' and destination=='SFO' and delay>0 and date like '1220%'""")).show()
    )

    # Joins
    print(
        delay_filter.join(airport, airport.IATA == delay_filter.origin)
        .select("City", "State", "date", "distance", "destination")
        .show()
    )

    # Windowing
    w = Window.partitionBy("origin").orderBy(F.desc("delay"))
    print(delay.withColumn("ranking", F.dense_rank().over(w)).show())

    # Pivot
    delay_pivot = (
        delay.withColumn("month", F.expr("CAST(SUBSTRING(date,0,1) as int)"))
        .filter(F.expr("origin='SEA'"))
        .select("destination", "month", "delay")
    )
    print(delay_pivot.groupBy("destination").pivot("month").sum("delay").show())

    spark.stop()
