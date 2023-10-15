"""Chapter 5 - user_defined_functions.py"""

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType


def cubed(s):
    """This function is registered as a UDF and will be used in Spark SQL"""
    return s * s * s


def cubed_pandas(a: pd.Series) -> pd.Series:
    """This function is registered as a Pandas UDF and will be used in Spark SQL"""
    return a * a * a


if __name__ == "__main__":
    spark = SparkSession.builder.appName("user_defined_functions").getOrCreate()

    # Default spark udf
    spark.udf.register("cubed", cubed, LongType())
    spark.range(1, 9).createOrReplaceTempView("udf_test")
    spark.sql("""SELECT id, cubed(id) as id_cubed from udf_test""").show()

    # Pandas udf (optimised)
    cubed_udf = pandas_udf(cubed_pandas, returnType=LongType())
    x = pd.Series([1, 2, 3])
    print(cubed(x))
    df = spark.range(1, 4)
    df.select("id", cubed_udf(col("id"))).show()

    spark.stop()
