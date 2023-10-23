"""Chapter 7 - configurations.py"""

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("configurations")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", "1")
        .config("spark.dynamicAllocation.maxExecutors", "20")
        .config("spark.dynamicAllocation.schedulerBacklogTimeout", "1m")
        .config("spark.dynamicAllocation.executorIdleTimeout", "2min")
        .getOrCreate()
    )

    print(spark.conf.get("spark.sql.shuffle.partitions"))
    spark.conf.set("spark.sql.shuffle.partitions", 5)
    print(spark.conf.get("spark.sql.shuffle.partitions"))

    print(spark.conf.get("spark.sql.files.maxPartitionBytes"))
    spark.conf.set("spark.sql.files.maxPartitionBytes", "209715200")
    print(spark.conf.get("spark.sql.files.maxPartitionBytes"))

    spark.stop()
