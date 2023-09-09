"""Chapter 2 - mnmcount.py"""

# spark-submit ./src/chapter_2/mnmcount.py
# docker-compose exec spark-master spark-submit ./src/chapter_2/mnmcount.py

from pyspark.sql import SparkSession


def get_mnm_count(df):
    """This function returns the count of M&Ms by state and color"""

    df_agg = df.groupBy("State", "Color").sum("Count").orderBy("sum(Count)", ascending=False)
    return df_agg


def get_mnm_count_ca(df):
    """This function returns the count of M&Ms by state and color for CA state"""

    df_agg = (
        df.select("State", "Color", "Count")
        .where(df.State == "CA")
        .groupBy("State", "Color")
        .sum("Count")
        .orderBy("sum(Count)", ascending=False)
    )
    return df_agg


if __name__ == "__main__":
    spark = SparkSession.builder.appName("MnMCount").getOrCreate()

    mnm_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/opt/bitnami/spark/custom_data/mnm_dataset.csv")
    )

    print(mnm_df.show(n=10, truncate=False))

    count_mnm_df = get_mnm_count(df=mnm_df)
    print(count_mnm_df.show(n=60, truncate=False))
    print(f"Total Rows {count_mnm_df.count()}\n")
    print(count_mnm_df.explain())

    ca_count_mnm_df = get_mnm_count_ca(df=mnm_df)
    print(ca_count_mnm_df.show(n=10, truncate=False))
    print(ca_count_mnm_df.explain())

    spark.stop()
