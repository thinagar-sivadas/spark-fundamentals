"""Test file for common_dataframe_operations.py"""

import datetime

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import IntegerType, StructField, StructType

from src.chapter3.common_dataframe_operations import (
    aggregation,
    projections_and_filters,
    rename_add_drop_cols,
)


def test_projections_and_filters(spark_session):
    """Test projections_and_filters function"""

    spark = spark_session
    df = spark.createDataFrame(
        data=[
            (2003235, "01/11/2002 01:51:44 AM", "Medical Incident"),
            (2003235, "01/11/2002 01:51:44 AM", "Structure Fire"),
        ],
        schema=["IncidentNumber", "AvailableDtTm", "CallType"],
    )
    df_few_fire, df_distinct_count, df_distinct = projections_and_filters(df=df)

    expected_df_few_fire = spark.createDataFrame(
        data=[
            (2003235, "01/11/2002 01:51:44 AM", "Structure Fire"),
        ],
        schema=["IncidentNumber", "AvailableDtTm", "CallType"],
    )

    expected_df_distinct_count = spark.createDataFrame(
        data=[
            (2,),
        ],
        schema=["DistinctCallTypes"],
    )

    expected_df_distinct = spark.createDataFrame(
        data=[("Medical Incident",), ("Structure Fire",)],
        schema=["CallType"],
    )

    assert_df_equality(df1=expected_df_few_fire, df2=df_few_fire)
    assert_df_equality(df1=expected_df_distinct_count, df2=df_distinct_count, ignore_nullable=True)
    assert_df_equality(df1=expected_df_distinct, df2=df_distinct)


def test_rename_add_drop_cols(spark_session):
    """Test rename_add_drop_cols function"""

    spark = spark_session
    df = spark.createDataFrame(
        data=[
            (5, "01/11/2002", "01/10/2002", "01/11/2002 01:51:44 AM"),
            (10, "01/11/2002", "01/10/2002", "01/11/2002 03:01:18 AM"),
        ],
        schema=["Delay", "CallDate", "WatchDate", "AvailableDtTm"],
    )
    df_new_fire_filter, df_fire_ts, df_fire_ts_order = rename_add_drop_cols(df=df)

    expected_df_new_fire_filter = spark.createDataFrame(
        data=[
            (10,),
        ],
        schema=["ResponseDelayedinMins"],
    )

    expected_df_fire_ts = spark.createDataFrame(
        data=[
            (
                5,
                datetime.datetime(2002, 1, 11),
                datetime.datetime(2002, 1, 10),
                datetime.datetime(2002, 1, 11, 1, 51, 44),
            ),
            (
                10,
                datetime.datetime(2002, 1, 11),
                datetime.datetime(2002, 1, 10),
                datetime.datetime(2002, 1, 11, 3, 1, 18),
            ),
        ],
        schema=["ResponseDelayedinMins", "IncidentDate", "OnWatchDate", "AvailableDtTS"],
    )

    expected_df_fire_ts_order = spark.createDataFrame(
        data=[
            (2002,),
        ],
        schema=StructType([StructField("year(IncidentDate)", IntegerType(), True)]),
    )

    assert_df_equality(df1=df_new_fire_filter, df2=expected_df_new_fire_filter)
    assert_df_equality(df1=df_fire_ts, df2=expected_df_fire_ts)
    assert_df_equality(df1=df_fire_ts_order, df2=expected_df_fire_ts_order)


def test_aggregation(spark_session):
    """Test aggregation function"""

    spark = spark_session
    df = spark.createDataFrame(
        data=[
            ("Structure Fire", 1, 2.95),
            ("Medical Incident", 1, 4.7),
            ("Medical Incident", 1, 2.5),
        ],
        schema=["CallType", "NumAlarms", "ResponseDelayedinMins"],
    )

    df_agg, df_agg_other = aggregation(df=df)

    expected_df_agg = spark.createDataFrame(
        data=[("Medical Incident", 2), ("Structure Fire", 1)], schema=["CallType", "count"]
    )

    expected_df_agg_other = spark.createDataFrame(
        data=[
            (3, 3.3833333333333333, 2.5, 4.7),
        ],
        schema=[
            "sum(NumAlarms)",
            "avg(ResponseDelayedinMins)",
            "min(ResponseDelayedinMins)",
            "max(ResponseDelayedinMins)",
        ],
    )

    assert_df_equality(df1=df_agg, df2=expected_df_agg, ignore_nullable=True)
    assert_df_equality(df1=df_agg_other, df2=expected_df_agg_other)
