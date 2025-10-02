from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format
import time
import os


kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
s3_endpoint = os.getenv('S3_ENDPOINT_URL', 'https://legendary-waddle-wpxjw6pjxp7f9pvp-4566.app.github.dev')
s3_bucket = os.getenv('DATA_LAKE_BUCKET', 'incircl')

spark = SparkSession.builder \
    .appName("EcommerceLogConsumer") \
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", "Stock") \
    .option("startingOffsets", "latest") \
    .load()

df_with_time = df.withColumn("processing_date", date_format(current_timestamp(), "yyyy-MM-dd")) \
                 .withColumn("processing_hour", date_format(current_timestamp(), "HH"))


output_path = f"s3://{s3_bucket}/bronze_layer/stream_job/events/"
checkpoint_path = f"s3://{s3_bucket}/bronze_layer/stream_job/events/checkpoint/"

query = df_with_time.selectExpr("CAST(value AS STRING)", "processing_date", "processing_hour") \
    .writeStream \
    .format("json") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .partitionBy("processing_date", "processing_hour") \
    .outputMode("append") \
    .start()


last_event_time = time.time()

try:
    while query.isActive:
        progress = query.lastProgress
        if progress and "numInputRows" in progress:
            if progress["numInputRows"] > 0:
                last_event_time = time.time()
                print(f"Processed {progress['numInputRows']} records")
        
        if time.time() - last_event_time > 10:
            print("No new events for 10s. Stopping query...")
            query.stop()
            break

        time.sleep(2)
except Exception as e:
    print(f"Error in streaming: {e}")
    if query.isActive:
        query.stop()
finally:
    if query.isActive:
        query.stop()
    spark.stop()  
