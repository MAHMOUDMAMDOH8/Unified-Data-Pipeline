import os
import sys
import logging
CURRENT_DIR = os.path.dirname(__file__)
DAGS_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, '..', '..'))
if DAGS_ROOT not in sys.path:
    sys.path.append(DAGS_ROOT)
from utils import create_spark_session, load_config
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, IntegerType, ArrayType
)
from pyspark.sql import functions as F


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('consumer')


order_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("level", StringType(), True),
    StructField("service", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("session_duration", LongType(), True),
    StructField("device_type", StringType(), True),
    StructField("geo_location", StructType([
        StructField("country", StringType(), True),
        StructField("city", StringType(), True)
    ])),
    StructField("details", StructType([
        StructField("order_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("order_amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("shipping_method", StringType(), True),
        StructField("shipping_address", StringType(), True),
        StructField("warehouse", StringType(), True),
        StructField("carrier", StringType(), True),
        StructField("tracking_number", StringType(), True),
        StructField("completed_at", StringType(), True),
        StructField("delivery_estimate", StringType(), True),
        StructField("items", ArrayType(StructType([
            StructField("product_id", LongType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("line_amount", DoubleType(), True),
            StructField("category", StringType(), True)
        ])))
    ]))
])


spark = create_spark_session()
load_config(spark.sparkContext)

logger.info("Starting Kafka stream consumer...")


import time
logger.info("Waiting 5 seconds for Producer to start sending data...")
time.sleep(5)

stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "Stock")
    .option("startingOffsets", "earliest")  
    .option("failOnDataLoss", "false")
    .load()
)

logger.info("Kafka stream created, processing data...")

event_df = stream_df.selectExpr("CAST(value AS STRING) AS raw_payload", "topic", "partition", "offset")


def debug_kafka_batch(df, batch_id):
    count = df.count()
    logger.info(f"Kafka debug batch {batch_id}: {count} records")
    if count > 0:
        logger.info("Kafka metadata:")
        df.select("topic", "partition", "offset").show(5, truncate=False)
        logger.info("Sample raw payload:")
        df.select("raw_payload").show(2, truncate=False)
    return df


kafka_debug_query = event_df.writeStream.foreachBatch(debug_kafka_batch).start()


payload_df = event_df.select("raw_payload")
parsed_df = payload_df.withColumn("payload", F.from_json("raw_payload", order_schema)).select("payload.*")


def debug_batch(df, batch_id):
    count = df.count()
    logger.info(f"Debug batch {batch_id}: {count} records")
    if count > 0:
        logger.info("Sample data:")
        df.show(2, truncate=False)
    return df


debug_query = parsed_df.writeStream.foreachBatch(debug_batch).start()

logger.info("Data parsing completed, starting main write stream...")


stream_query = (
    parsed_df.writeStream
    .format("json")
    .outputMode("append")
    .trigger(processingTime="1 minute")
    .option("path", "s3a://incircl/bronze_layer/stream_job/events/")
    .option("checkpointLocation", "s3a://incircl/bronze_layer/stream_job/events/checkpointDir")
    .start()
)


timeout_secs_str = os.getenv("STREAM_AWAIT_TIMEOUT_SECS", "180")
try:
    timeout_secs = int(timeout_secs_str)
except ValueError:
    timeout_secs = 90

logger.info(f"Waiting for streams to process data for {timeout_secs} seconds...")

terminated = stream_query.awaitTermination(timeout=timeout_secs)
if not terminated:
    logger.info("Timeout reached, stopping streams...")
    try:
        stream_query.stop()
        debug_query.stop()
        kafka_debug_query.stop()
    finally:
        spark.stop()
else:
    logger.info("Stream processing completed successfully")
    debug_query.stop()
    kafka_debug_query.stop()
    spark.stop()
