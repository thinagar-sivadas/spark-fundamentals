"""Chapter 3 - create_schema_dataframe.py"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def create_schema_ddl():
    """This function returns the schema for the dataframe"""

    ddl_schema = (
        "`Id` INT, `First` STRING, `Last` STRING,"
        + "`Url` STRING, `Published` STRING, `Hits` INT, `Campaigns` ARRAY<STRING>"
    )
    return ddl_schema


def create_schema_pyspark():
    """This function returns the schema for the dataframe"""

    pyspark_schema = StructType(
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
    return pyspark_schema


if __name__ == "__main__":
    spark = SparkSession.builder.appName("create_schema_dataframe").getOrCreate()

    blogs_df_ddl = (
        spark.read.option("multiline", "true")
        .format("json")
        .schema(schema=create_schema_ddl())
        .load("/opt/bitnami/spark/custom_data/chapter3/blogs.json")
    )
    print(blogs_df_ddl.show())

    blogs_df_pyspark = (
        spark.read.option("multiline", "true")
        .format("json")
        .schema(schema=create_schema_pyspark())
        .load("/opt/bitnami/spark/custom_data/chapter3/blogs.json")
    )
    print(blogs_df_pyspark.show())

    spark.stop()
