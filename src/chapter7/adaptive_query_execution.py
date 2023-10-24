"""Chapter 7 - adaptive_query_execution.py"""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("adaptive_query_execution").getOrCreate()
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    # Skewed dataset with skewed join
    df_trans = spark.read.format("parquet").load("/opt/bitnami/spark/custom_data/chapter7/transactions/")
    df_cust = spark.read.format("parquet").load("/opt/bitnami/spark/custom_data/chapter7/customers/")
    print(
        df_trans.groupBy("cust_id")
        .agg(F.countDistinct("txn_id").alias("ct"))
        .orderBy(F.desc("ct"))
        .show(5, truncate=False)
    )
    df_txn_details = df_trans.join(df_cust, on="cust_id", how="inner")
    print(df_txn_details.count())

    # Using AQE
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewdedJoin.enabled", "true")
    df_txn_details = df_trans.join(df_cust, on="cust_id", how="inner")
    print(df_txn_details.count())

    spark.stop()
