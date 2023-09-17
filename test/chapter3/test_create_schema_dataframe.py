"""Test file for create_schema_dataframe.py"""

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.chapter3.create_schema_dataframe import (
    create_schema_ddl,
    create_schema_pyspark,
)


def test_create_schema_ddl(spark_session):
    """Test create_schema_ddl function"""

    spark = spark_session
    data = [(1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"])]
    # pylint: disable=R0801
    schema = StructType(
        [
            StructField("Id", IntegerType(), True),
            StructField("First", StringType(), True),
            StructField("Last", StringType(), True),
            StructField("Url", StringType(), True),
            StructField("Published", StringType(), True),
            StructField("Hits", IntegerType(), True),
            StructField("Campaigns", ArrayType(elementType=StringType()), True),
        ]
    )
    expected_df = spark.createDataFrame(
        data=data,
        schema=schema,
    )
    data_df = spark.createDataFrame(data=data, schema=create_schema_ddl())
    assert_df_equality(df1=expected_df, df2=data_df)


def test_create_schema_pyspark(spark_session):
    """Test create_schema_pyspark function"""

    spark = spark_session
    data = [(1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"])]
    schema = (
        "`Id` INT, `First` STRING, `Last` STRING,"
        + "`Url` STRING, `Published` STRING, `Hits` INT, `Campaigns` ARRAY<STRING>"
    )
    expected_df = spark.createDataFrame(
        data=data,
        schema=schema,
    )
    data_df = spark.createDataFrame(data=data, schema=create_schema_pyspark())
    assert_df_equality(df1=expected_df, df2=data_df)
