import os
from pyspark.sql import SparkSession


def create_spark_session(app_name="Stream Consumer"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.apache.spark:spark-hadoop-cloud_2.13:3.5.1"
        )
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.multipart.purge", "true")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.fs.s3a.committer.name", "directory")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .getOrCreate()
    )
    return spark


def load_config(sc):
    """Configure Hadoop for S3 access"""
    hconf = sc._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.access.key", "test")
    hconf.set("fs.s3a.secret.key", "test")

    endpoint = os.getenv(
        "S3_ENDPOINT_URL",
        "https://legendary-waddle-wpxjw6pjxp7f9pvp-4566.app.github.dev"
    )
    hconf.set("fs.s3a.endpoint", endpoint)
    hconf.set("fs.s3a.connection.ssl.enabled", "true" if endpoint.startswith("https") else "false")
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    hconf.set("fs.s3a.attempts.maximum", "3")
    hconf.set("fs.s3a.connection.establish.timeout", "60000")
    hconf.set("fs.s3a.connection.timeout", "60000")
    hconf.set("fs.s3a.connection.request.timeout", "60000")
    hconf.set("fs.s3a.socket.timeout", "60000")
    hconf.set("fs.s3a.threads.keepalivetime", "60")
    hconf.set("fs.s3a.multipart.purge", "true")
    hconf.set("fs.s3a.multipart.purge.age", "86400")
    hconf.set("fs.s3a.paging.maximum", "6000")
    hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
