"""Chapter 7 - cache.py"""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("cache").getOrCreate()

    df_customers = (
        spark.read.format("parquet").option("header", "true").load("/opt/bitnami/spark/custom_data/chapter7/customers/")
    )

    print(df_customers.show())

    df_base = (
        df_customers.filter(F.col("city") == "boston")
        .withColumn(
            "customer_group",
            F.when(F.col("age").between(20, 30), F.lit("young"))
            .when(F.col("age").between(31, 50), F.lit("mid"))
            .when(F.col("age") > 51, F.lit("old"))
            .otherwise(F.lit("kid")),
        )
        .select("cust_id", "name", "age", "gender", "birthday", "zip", "city", "customer_group")
    )

    print(df_base.show(5, truncate=False))

    # without cache
    df1 = df_base.withColumn("test_column_1", F.lit("test_column_1")).withColumn(
        "birth_year", F.split("birthday", "/").getItem(2)
    )
    print(df1.explain())
    print(df1.show(5, truncate=False))

    df2 = df_base.withColumn("test_column_2", F.lit("test_column_2")).withColumn(
        "birth_month", F.split("birthday", "/").getItem(1)
    )
    print(df2.explain())
    print(df2.show(5, truncate=False))

    # with cache
    df_base.cache()

    df1 = df_base.withColumn("test_column_1", F.lit("test_column_1")).withColumn(
        "birth_year", F.split("birthday", "/").getItem(2)
    )

    print(df1.explain())
    print(df1.show(5, truncate=False))

    df2 = df_base.withColumn("test_column_2", F.lit("test_column_2")).withColumn(
        "birth_month", F.split("birthday", "/").getItem(1)
    )
    print(df2.explain())
    print(df2.show(5, truncate=False))

    spark.stop()
