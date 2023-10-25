"""Chapter 7 - broadcast_join.py"""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("broadcast_join").getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)

    # Skewed dataset with skewed join
    df_trans = spark.read.format("parquet").load("/opt/bitnami/spark/custom_data/chapter7/transactions/")
    df_cust = spark.read.format("parquet").load("/opt/bitnami/spark/custom_data/chapter7/customers/")
    df_txn_details = df_trans.join(F.broadcast(df_cust), on="cust_id", how="inner")
    print(df_txn_details.count())

    spark.stop()
