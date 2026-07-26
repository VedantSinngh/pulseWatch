FROM python:3.11-slim

# System deps for PySpark: Java + curl
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for PySpark
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY streaming_job.py .

# Pre-download the Spark-Kafka JAR on image build so the container
# doesn't need internet access at runtime
RUN python -c "\
from pyspark.sql import SparkSession; \
spark = SparkSession.builder \
  .master('local[1]') \
  .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1') \
  .getOrCreate(); \
spark.stop()" || true

CMD ["python", "-u", "streaming_job.py"]
