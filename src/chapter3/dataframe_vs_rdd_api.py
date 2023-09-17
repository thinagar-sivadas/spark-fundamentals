"""Chapter 3 - dataframe_vs_rdd_api.py"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg


def get_rdd_avg_age(rdd):
    """This function returns the average age of the RDD"""

    rdd_agg = (
        rdd.map(lambda x: (x[0], (x[1], 1)))
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
        .map(lambda x: (x[0], x[1][0] / x[1][1]))
    )
    return rdd_agg


def get_df_avg_age(df):
    """This function returns the average age of the dataframe"""

    df_agg = df.groupBy("name").agg(avg("age"))
    return df_agg


if __name__ == "__main__":
    spark = SparkSession.builder.appName("dataframe_vs_rdd_api").getOrCreate()

    dataRDD = spark.sparkContext.parallelize([("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)])

    agesRDD = get_rdd_avg_age(rdd=dataRDD)
    print(agesRDD.collect())

    data_df = spark.createDataFrame(
        data=[("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)], schema=["name", "age"]
    )

    avg_df = get_df_avg_age(df=data_df)
    print(avg_df.show())

    spark.stop()
