"""Chapter 7 - salting.py"""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

if __name__ == "__main__":
    spark = SparkSession.builder.appName("salting").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "3")
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    # Uniform dataset
    df_unif = spark.createDataFrame(list(range(1000000)), IntegerType())
    print(df_unif.show(3, truncate=False))
    print(
        df_unif.withColumn("partition", F.spark_partition_id()).groupBy("partition").count().orderBy("partition").show()
    )

    # Skewed dataset
    df0 = spark.createDataFrame([0] * 999990, IntegerType()).repartition(1)
    df1 = spark.createDataFrame([1] * 15, IntegerType()).repartition(1)
    df2 = spark.createDataFrame([2] * 10, IntegerType()).repartition(1)
    df3 = spark.createDataFrame([3] * 5, IntegerType()).repartition(1)
    df_skew = df0.union(df1).union(df2).union(df3)
    print(df_skew.show(3, truncate=False))
    print(
        df_skew.withColumn("partition", F.spark_partition_id()).groupBy("partition").count().orderBy("partition").show()
    )

    # Skewed join dataset
    df_joined_c1 = df_skew.join(df_unif, "value", "inner")
    print(df_joined_c1.withColumn("partition", F.spark_partition_id()).groupBy("partition").count().show())

    # Simulating uniform distribution through salting
    SALT_NUMBER = int(spark.conf.get("spark.sql.shuffle.partitions"))
    print(SALT_NUMBER)
    df_skew = df_skew.withColumn("salt", (F.rand() * SALT_NUMBER).cast("int"))
    print(df_skew.show())
    df_unif = df_unif.withColumn("salt_values", F.array([F.lit(i) for i in range(SALT_NUMBER)])).withColumn(
        "salt", F.explode(F.col("salt_values"))
    )
    print(df_unif.show())
    df_joined = df_skew.join(df_unif, ["value", "salt"], "inner")
    print(
        df_joined.withColumn("partition", F.spark_partition_id())
        .groupBy("value", "partition")
        .count()
        .orderBy("value", "partition")
        .show()
    )

    # Salting in aggregation
    print(
        df_skew.groupBy("value", "salt")
        .agg(F.count("value").alias("count"))
        .groupBy("value")
        .agg(F.sum("count").alias("count"))
        .show()
    )

    spark.stop()
