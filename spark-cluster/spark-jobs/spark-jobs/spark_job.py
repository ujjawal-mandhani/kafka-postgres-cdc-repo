from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *
import re

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "my_prefix.public.customers"

spark = SparkSession.builder\
  .appName("read_stream").getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
  .option("subscribe", KAFKA_TOPIC) \
  .load()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

query = df \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/home/data") \
    .option("checkpointLocation", "/home/checkpoint") \
    .start()

query.awaitTermination()
