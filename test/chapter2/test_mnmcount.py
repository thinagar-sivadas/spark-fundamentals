"""Test file for mnmcount.py"""

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import LongType, StringType, StructField, StructType

from src.chapter2.mnmcount import get_mnm_count, get_mnm_count_ca


def test_get_mnm_count(spark_session):
    """Test get_mnm_count function"""

    spark = spark_session
    mnm_df = spark.createDataFrame(
        data=[
            ("Singapore", "red", 1),
            ("Singapore", "red", 2),
        ],
        schema=["State", "Color", "Count"],
    )
    expected_df = spark.createDataFrame(
        data=[
            ("Singapore", "red", 3),
        ],
        schema=["State", "Color", "sum(Count)"],
    )
    count_mnm_df = get_mnm_count(df=mnm_df)
    assert_df_equality(df1=expected_df, df2=count_mnm_df)


def test_get_mnm_count_ca(spark_session):
    """Test get_mnm_count_ca function"""
    spark = spark_session
    schema = StructType(
        [
            StructField("State", StringType(), True),
            StructField("Color", StringType(), True),
            StructField("sum(Count)", LongType(), True),
        ]
    )
    mnm_df = spark.createDataFrame(
        data=[
            ("Singapore", "red", 1),
            ("Singapore", "red", 2),
        ],
        schema=["State", "Color", "Count"],
    )
    expected_df = spark.createDataFrame(
        data=[],
        schema=schema,
    )
    ca_count_mnm_df = get_mnm_count_ca(df=mnm_df)
    assert_df_equality(df1=expected_df, df2=ca_count_mnm_df)
