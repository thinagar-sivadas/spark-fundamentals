"""Chapter 3 - common_dataframe_operations.py"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg as spark_avg
from pyspark.sql.functions import col, countDistinct
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import to_timestamp, year
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def projections_and_filters(df):
    """Projections and filters"""

    df_few_fire = df.select("IncidentNumber", "AvailableDtTm", "CallType").where(col("CallType") != "Medical Incident")
    df_distinct_count = (
        df.select("CallType")
        .where(col("CallType").isNotNull())
        .agg(countDistinct("CallType").alias("DistinctCallTypes"))
    )
    df_distinct = df.select("CallType").where(col("CallType").isNotNull()).distinct().alias("DistinctCallTypes")

    return df_few_fire, df_distinct_count, df_distinct


def rename_add_drop_cols(df):
    """Rename, add and drop columns"""

    df_new_fire = df.withColumnRenamed("Delay", "ResponseDelayedinMins")
    df_new_fire_filter = df_new_fire.select("ResponseDelayedinMins").where(col("ResponseDelayedinMins") > 5)

    df_fire_ts = (
        df_new_fire.withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
        .drop("CallDate")
        .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
        .drop("WatchDate")
        .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
        .drop("AvailableDtTm")
    )

    df_fire_ts_order = df_fire_ts.select(year("IncidentDate")).distinct().orderBy(year("IncidentDate"))

    return df_new_fire_filter, df_fire_ts, df_fire_ts_order


def aggregation(df):
    """Aggregation"""

    df_agg = (
        df.select("CallType")
        .where(col("CallType").isNotNull())
        .groupBy("CallType")
        .count()
        .orderBy("count", ascending=False)
    )

    df_agg_other = df.select(
        spark_sum("NumAlarms"),
        spark_avg("ResponseDelayedinMins"),
        spark_min("ResponseDelayedinMins"),
        spark_max("ResponseDelayedinMins"),
    )

    return df_agg, df_agg_other


if __name__ == "__main__":
    spark = SparkSession.builder.appName("common_dataframe_operations").getOrCreate()
    # Create schema
    schema = StructType(
        [
            StructField("CallNumber", IntegerType(), True),
            StructField("UnitID", StringType(), True),
            StructField("IncidentNumber", IntegerType(), True),
            StructField("CallType", StringType(), True),
            StructField("CallDate", StringType(), True),
            StructField("WatchDate", StringType(), True),
            StructField("CallFinalDisposition", StringType(), True),
            StructField("AvailableDtTm", StringType(), True),
            StructField("Address", StringType(), True),
            StructField("City", StringType(), True),
            StructField("Zipcode", IntegerType(), True),
            StructField("Battalion", StringType(), True),
            StructField("StationArea", StringType(), True),
            StructField("Box", StringType(), True),
            StructField("OriginalPriority", StringType(), True),
            StructField("Priority", StringType(), True),
            StructField("FinalPriority", IntegerType(), True),
            StructField("ALSUnit", BooleanType(), True),
            StructField("CallTypeGroup", StringType(), True),
            StructField("NumAlarms", IntegerType(), True),
            StructField("UnitType", StringType(), True),
            StructField("UnitSequenceInCallDispatch", IntegerType(), True),
            StructField("FirePreventionDistrict", StringType(), True),
            StructField("SupervisorDistrict", StringType(), True),
            StructField("Neighborhood", StringType(), True),
            StructField("Location", StringType(), True),
            StructField("RowID", StringType(), True),
            StructField("Delay", FloatType(), True),
        ]
    )
    # Read DataFrame
    fire_df = (
        spark.read.format("csv")
        .schema(schema)
        .option("header", "true")
        .load("/opt/bitnami/spark/custom_data/chapter3/sf-fire-calls.csv")
    )
    print(fire_df.show(5, truncate=False))

    # Save DataFrame
    fire_df.write.format("parquet").mode("overwrite").option(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
    ).save("/opt/bitnami/spark/custom_data/chapter3/output/")

    # Save DataFrame create table
    fire_df.write.format("parquet").mode("overwrite").option(
        "path", "/opt/bitnami/spark/custom_data/chapter3/output/"
    ).option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false").saveAsTable("fire_df")

    few_fire_df, distinct_count_df, distinct_df = projections_and_filters(df=fire_df)
    print(few_fire_df.show(5, truncate=False))
    print(distinct_count_df.show(5, truncate=False))
    print(distinct_df.show(5, truncate=False))

    new_fire_filter_df, fire_ts_df, fire_ts_order_df = rename_add_drop_cols(df=fire_df)
    print(new_fire_filter_df.show(5, truncate=False))
    print(fire_ts_df.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(5, truncate=False))
    print(fire_ts_order_df.show(5, truncate=False))

    fire_ts_agg_df, fire_ts_agg_other_df = aggregation(df=fire_ts_df)
    print(fire_ts_agg_df.show(5, truncate=False))
    print(fire_ts_agg_other_df.show(5, truncate=False))
