"""Chapter 2 - mnmcount.py"""

# spark-submit ./src/chapter_2/mnmcount.py
# docker-compose exec spark-master spark-submit ./src/chapter_2/mnmcount.py

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MnMCount").getOrCreate()

mnm_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/opt/bitnami/spark/custom_data/mnm_dataset.csv")
)

print(mnm_df.show(n=10, truncate=False))

count_mnm_df = mnm_df.groupBy("State", "Color").sum("Count").orderBy("sum(Count)", ascending=False)
print(count_mnm_df.show(n=60, truncate=False))
print(f"Total Rows {count_mnm_df.count()}\n")
print(count_mnm_df.explain())

ca_count_mnm_df = (
    mnm_df.select("State", "Color", "Count")
    .where(mnm_df.State == "CA")
    .groupBy("State", "Color")
    .sum("Count")
    .orderBy("sum(Count)", ascending=False)
)
print(ca_count_mnm_df.show(n=10, truncate=False))
print(ca_count_mnm_df.explain())

spark.stop()
