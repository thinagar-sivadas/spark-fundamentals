# Use the bitnami/spark image as the base image
FROM bitnami/spark:latest

USER root

# ENV PATH="/opt/bitnami/python/bin:$PATH"

# Copy the requirements.txt file to the container
COPY requirements.txt .
COPY ./spark-defaults.conf "$SPARK_HOME/conf"
COPY ./log4j2.properties "$SPARK_HOME/conf"

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir notebook
