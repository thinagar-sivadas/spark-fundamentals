"""Chapter 12 - user_defined_functions.py"""

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf


@pandas_udf("long")
def pandas_plus_one(v: pd.Series) -> pd.Series:
    """This function adds one to the input series and returns it"""
    return v + 1


if __name__ == "__main__":
    spark = SparkSession.builder.appName("user_defined_functions").getOrCreate()

    # Pandas udf (optimised)
    df = spark.range(3)
    df.withColumn("plus_one", pandas_plus_one("id")).show()

    spark.stop()
