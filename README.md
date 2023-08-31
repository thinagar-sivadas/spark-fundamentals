# spark-fundamentals
Discover Apache Spark's core concepts and applications. Explore tutorials, examples, and code snippets for beginners and experienced developers. Master data processing and analysis through hands-on guidance. Elevate your skills in this essential open-source big data framework

Instructions:
docker build -t custom_spark .

Start app:
docker-compose up -d
docker-compose down -v

Run Code:
spark-submit ./src/scripts/test.py
docker-compose exec spark-master spark-submit ./src/script/test.py
