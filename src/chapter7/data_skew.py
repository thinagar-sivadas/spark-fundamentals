"""Chapter 7 - data_skew.py"""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("data_skew").getOrCreate()
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    # Uniform dataset
    df_uniform = spark.range(1000000)
    print(df_uniform.show(3, truncate=False))

    print(
        df_uniform.withColumn("partition", F.spark_partition_id())
        .groupBy("partition")
        .count()
        .orderBy("partition")
        .show()
    )

    # Skewed dataset
    df0 = spark.range(0, 1000000).repartition(1)
    df1 = spark.range(0, 10).repartition(1)
    df2 = spark.range(0, 10).repartition(1)
    df_skew = df0.union(df1).union(df2)
    print(df_skew.show(3, truncate=False))

    print(
        df_skew.withColumn("partition", F.spark_partition_id()).groupBy("partition").count().orderBy("partition").show()
    )

    # Skewed dataset with skewed join
    df_transactions = spark.read.format("parquet").load("/opt/bitnami/spark/custom_data/chapter7/transactions/")
    df_customers = spark.read.format("parquet").load("/opt/bitnami/spark/custom_data/chapter7/customers/")
    print(
        df_transactions.groupBy("cust_id")
        .agg(F.countDistinct("txn_id").alias("ct"))
        .orderBy(F.desc("ct"))
        .show(5, truncate=False)
    )
    df_txn_details = df_transactions.join(df_customers, on="cust_id", how="inner")
    print(df_txn_details.count())

    spark.stop()
