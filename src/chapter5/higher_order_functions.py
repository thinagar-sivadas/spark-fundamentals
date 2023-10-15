"""Chapter 5 - higher_order_functions.py"""

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, IntegerType, StructField, StructType

if __name__ == "__main__":
    spark = SparkSession.builder.appName("higher_order_functions").getOrCreate()

    # Create DataFrame
    schema = StructType([StructField("celsius", ArrayType(IntegerType()))])
    t_list = [[35, 36, 32, 30]], [[31, 32, 34, 55, 56]]
    t_c = spark.createDataFrame(t_list, schema)
    t_c.show()
    t_c.createOrReplaceTempView("tC")

    # Apply transform on ArrayType column
    print(spark.sql("select celsius, transform(celsius, t-> (t*9) div 5 + 32) as farenheit from tC").show())

    # Filter ArrayType column
    print(spark.sql("select celsius, filter(celsius, t -> t>38) as high from tC").show())

    # Check for existence of value in ArrayType column
    print(spark.sql("select celsius, exists(celsius, t -> t=35) as threshold from tC").show())

    # Reduce values in ArrayType column
    print(
        spark.sql(
            """select celsius,
                reduce(celsius,
                        0,
                        (t, acc) -> t+acc,
                        acc -> (acc div size(celsius)) * 4
                    ) as avgFarenheit
                from tC
            """
        ).show()
    )

    spark.stop()
