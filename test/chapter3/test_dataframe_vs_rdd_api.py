"""Test file for dataframe_vs_rdd_api.py"""

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from src.chapter3.dataframe_vs_rdd_api import get_df_avg_age, get_rdd_avg_age


def test_get_rdd_avg_age(spark_session):
    """Test get_rdd_avg_age function"""

    spark = spark_session
    dataRDD = spark.sparkContext.parallelize([("Brooke", 20), ("Denny", 31), ("Brooke", 22)])
    expectedRDD = spark.sparkContext.parallelize(
        [
            ("Brooke", 21.0),
            ("Denny", 31.0),
        ]
    )
    agesRDD = get_rdd_avg_age(rdd=dataRDD)
    assert agesRDD.collect() == expectedRDD.collect()


def test_get_df_avg_age(spark_session):
    """Test get_df_avg_age function"""

    spark = spark_session
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("avg(age)", DoubleType(), True),
        ]
    )
    data_df = spark.createDataFrame(
        data=[("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)], schema=["name", "age"]
    )

    expected_df = spark.createDataFrame(
        data=[("Brooke", 22.5), ("Denny", 31.0), ("Jules", 30.0), ("TD", 35.0)],
        schema=schema,
    )

    avg_df = get_df_avg_age(df=data_df)
    assert_df_equality(df1=expected_df, df2=avg_df)
