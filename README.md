![HitCount](https://img.shields.io/endpoint?url=https%3A%2F%2Fhits.dwyl.com%2FThinagar-Sivadas%2Fspark-fundamentals.json%3Fcolor%3Dpink)

# Spark Fundamentals
Discover Apache Spark's core concepts and applications. Explore tutorials, examples, and code snippets for beginners and experienced developers. Master data processing and analysis through hands-on guidance. Elevate your skills in this essential open-source big data framework

## Prerequisites
Install the following tools globally in your environment
- docker
- docker-compose
- python >= 3.8

## Installation
```bash
# First time setup
git clone https://github.com/Thinagar-Sivadas/spark-fundamentals.git
cd spark-fundamentals
pip install -r requirements.txt
docker build --platform linux/amd64 -t custom_spark . # For mac users
docker build -t custom_spark . # For all other users

# Run the container with 1 worker
docker-compose up -d
# Run the container with multiple workers
docker-compose up -d --scale spark-worker=2
# Stop and remove the container and volumes. This will purge the history server data
docker-compose down -v
# Stop the container
docker-compose stop
```

## Usage
This links can only be accessed after the docker container is up and running

[Spark Driver Notebook](http://localhost:8888) (_Allows running the spark applications interactively_)

[Spark History Server UI](http://localhost:18080) (_Allows viewing both historical and running spark application_)

[Spark Master UI](http://localhost:8080) (_This link shows basic information about the cluster_)

<!-- [Spark UI](http://localhost:4040) (_This link can only be accessed while the spark application is running_) -->

## Tutorials
### Chapter 2
>**Counting M&Ms for the Cookie Monster**

Demonstrates the basic concepts of Spark using the M&M's® candy data set.

To run in interactive mode:
http://localhost:8888/notebooks/src/chapter2/mnmcount.ipynb

To run in batch mode:
```bash
docker-compose exec spark-master spark-submit ./src/chapter2/mnmcount.py
```
### Chapter 3
>**DataFrame vs RDD API**

Demonstrates the differences between the DataFrame and RDD APIs. The DataFrame API is the recommended API for most use cases due to high-level abstractions and optimizations.

To run in interactive mode:
http://localhost:8888/notebooks/src/chapter3/dataframe_vs_rdd_api.ipynb

To run in batch mode:
```bash
docker-compose exec spark-master spark-submit ./src/chapter3/dataframe_vs_rdd_api.py
```
>**Schemas and Creating DataFrame**

Demonstrates how to create a DataFrame from a schema. Schema offers a way to define the structure of the data in a table. It is recommended as it relieve spark from inferring the schema (*expensive operation for large table*) from the data. Additionally, it allows for error checking at compile time.

To run in interactive mode:
http://localhost:8888/notebooks/src/chapter3/create_schema_dataframe.ipynb

To run in batch mode:
```bash
docker-compose exec spark-master spark-submit ./src/chapter3/create_schema_dataframe.py
```
>**Common DataFrame Operations**

Demonstrates the common DataFrame operations such as select, filter, where, groupBy, orderBy, withColumnRenamed. Showcase save operations such as saveAsTable and save. These operations are the building blocks for data processing and analysis.

To run in interactive mode:
http://localhost:8888/notebooks/src/chapter3/common_dataframe_operations.ipynb

To run in batch mode:
```bash
docker-compose exec spark-master spark-submit ./src/chapter3/common_dataframe_operations.py
```
### Chapter 4
>**Spark SQL**

Demonstrates the Spark SQL API. Spark SQL is a higher-level abstraction built on top of the DataFrame API. It allows for writing SQL queries to process and analyze data. Shows how to create database, tables, and views.

To run in interactive mode:
http://localhost:8888/notebooks/src/chapter4/spark_sql.ipynb

To run in batch mode:
```bash
docker-compose exec spark-master spark-submit ./src/chapter4/spark_sql.py
```
### Chapter 5
>**User Defined Functions**

Demonstrates on how to create and use user defined functions (UDF). UDFs are used to extend the functionality of Spark SQL. It allows for writing custom functions in Python. Additionally, it shows how to create pandas UDFs which are vectorized UDFs that are executed in parallel. It is the preferred way to write UDFs as it is faster than regular UDFs.

To run in interactive mode:
http://localhost:8888/notebooks/src/chapter5/user_defined_functions.ipynb

To run in batch mode:
```bash
docker-compose exec spark-master spark-submit ./src/chapter5/user_defined_functions.py
```
>**Higher Order Functions**

Demonstrates some of the higher order functions available in Spark SQL. These functions are used to manipulate arrays and maps. It allows for writing complex data processing and analysis logic in a single SQL statement.

To run in interactive mode:
http://localhost:8888/notebooks/src/chapter5/higher_order_functions.ipynb

To run in batch mode:
```bash
docker-compose exec spark-master spark-submit ./src/chapter5/higher_order_functions.py
```
>**External Datasources and Common Operations**

Demonstrates connection to postgres database and reading data from it. It also shows how to write data to postgres database. Additionally, common dataframe operation such as union, joins, window functions, and pivot are demonstrated.

To run in interactive mode:
http://localhost:8888/notebooks/src/chapter5/external_datasources_common_operations.ipynb

To run in batch mode:
```bash
docker-compose exec spark-master spark-submit --driver-class-path /opt/bitnami/spark/connectors/jars/postgresql-42.6.0.jar ./src/chapter5/external_datasources_common_operations.py
```
### Chapter 7
>**Configurations**

Demonstrates on how to configure spark application. It shows how to enable dynamic allocation, set the number of executors, and set the number of cores per executor. Additionally, it shows how to set the number of partitions for a dataframe and the byte size of each partition.

To run in interactive mode:
http://localhost:8888/notebooks/src/chapter7/configurations.ipynb

To run in batch mode:
```bash
docker-compose exec spark-master spark-submit ./src/chapter7/configurations.py
```
>**Cache**

Demonstrates how to cache data in memory. Caching data in memory is a common optimization technique to improve performance. It is recommended to cache data that is used multiple times in the application.

To run in interactive mode:
http://localhost:8888/notebooks/src/chapter7/cache.ipynb

To run in batch mode:
```bash
docker-compose exec spark-master spark-submit ./src/chapter7/cache.py
```
>**Data Skew**

Demonstrates the definition of data skew and how to identify it.

To run in interactive mode:
http://localhost:8888/notebooks/src/chapter7/data_skew.ipynb

To run in batch mode:
```bash
docker-compose exec spark-master spark-submit ./src/chapter7/data_skew.py
```
>**Adaptive Query Execution (AQE)**

Demonstrates the adaptive query execution feature. AQE is a feature that allows spark to dynamically optimize the execution plan based on the data. It is recommended to enable this feature as it improves performance.

To run in interactive mode:
http://localhost:8888/notebooks/src/chapter7/adaptive_query_execution.ipynb

To run in batch mode:
```bash
docker-compose exec spark-master spark-submit ./src/chapter7/adaptive_query_execution.py
```
>**Broadcast Join**

Demonstrates broadcast join. Broadcast join is a join optimization technique that allows spark to broadcast the smaller table to all the executors. It is recommended to use this technique when the smaller table can fit in memory.

To run in interactive mode:
http://localhost:8888/notebooks/src/chapter7/broadcast_join.ipynb

To run in batch mode:
```bash
docker-compose exec spark-master spark-submit ./src/chapter7/broadcast_join.py
```
>**Salting**

Demonstrates how to use salting to avoid data skew. Salting is a technique that allows spark to distribute the data evenly across partitions. It is recommended to use this technique when the data is skewed

To run in interactive mode:
http://localhost:8888/notebooks/src/chapter7/salting.ipynb

To run in batch mode:
```bash
docker-compose exec spark-master spark-submit ./src/chapter7/salting.py
```

## Contributing
Pull requests are welcome. Adding of examples and tutorials are highly appreciated. Ensure pre-commit hooks are installed before committing the code. This will ensure the code is linted and formatted before committing. Include pytest unit tests for the code you add.
- Prerequisites
    - Install JDK
    - Set the `JAVA_HOME` environment variable
    - Set the `PYSPARK_PYTHON` environment variable
```bash
# First time setup
git clone https://github.com/Thinagar-Sivadas/spark-fundamentals.git
cd spark-fundamentals
pip install -r requirements.txt
pre-commit install
```

## Acknowledgements
 - [Learning Spark (Lightning-Fast Data Analytics), 2nd Edition](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/)
