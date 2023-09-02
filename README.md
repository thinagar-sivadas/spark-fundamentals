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

# For mac users
docker build --platform linux/amd64 -t custom_spark .
# For all other users
docker build -t custom_spark .

# Run the container
docker-compose up -d
# Stop the container
docker-compose down -v
```

## Usage
This links can only be accessed after the docker container is up and running

[Spark Driver Notebook](http://localhost:8888) (_Allows running the spark applications interactively_)

[Spark History Server UI](http://localhost:18080) (_Allows viewing both historical and running spark application_)

[Spark Master UI](http://localhost:8080) (_This link shows basic information about the cluster_)

<!-- [Spark UI](http://localhost:4040) (_This link can only be accessed while the spark application is running_) -->


## Tutorials
### Chapter 2
WIP
<!-- [Topics](https://linktodocumentation) -->


## Acknowledgements
 - [Learning Spark (Lightning-Fast Data Analytics), 2nd Edition](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/)
