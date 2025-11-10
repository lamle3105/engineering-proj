# src/common/spark_utils.py
"""
Utilities to create a SparkSession configured for S3A/MinIO on Windows.

- Spark:   4.0.1
- Hadoop:  3.3.4
- MinIO:   http://localhost:9000 (path-style)
- AWS SDK: 1.12.691

Notes:
* Hadoop S3A options that represent durations must be numeric (no "s", "m", "h").
* This file overrides common pitfalls like "60s" or "24h" with integer milliseconds/seconds.
"""

from typing import Dict, Optional

from pyspark.sql import SparkSession

HADOOP_AWS_VER = "3.3.4"
AWS_SDK_BUNDLE_VER = "1.12.691"

DEFAULT_S3_ENDPOINT = "http://localhost:9000"
DEFAULT_S3_ACCESS_KEY = "minioadmin"
DEFAULT_S3_SECRET_KEY = "minioadmin"


def _apply_confs(builder: SparkSession.Builder, confs: Dict[str, str]) -> SparkSession.Builder:
    for k, v in confs.items():
        builder = builder.config(k, v)
    return builder


def get_spark(app_name="DataPipeline"):
    return (
        SparkSession.builder
        .appName(app_name)

        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")

        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.multipart.purge", "true")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")
        .config("spark.hadoop.fs.s3a.multipart.purge.interval", "3600000")
        
        # Improved settings for MinIO compatibility
        .config("spark.hadoop.fs.s3a.retry.interval", "1000")
        .config("spark.hadoop.fs.s3a.retry.limit", "10")
        .config("spark.hadoop.fs.s3a.retry.throttle.interval", "1000")
        .config("spark.hadoop.fs.s3a.retry.throttle.limit", "10")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
        
        # Better handling of list operations (MinIO can be sensitive)
        .config("spark.hadoop.fs.s3a.list.parallelism", "8")
        .config("spark.hadoop.fs.s3a.list.version", "2")
        
        # File output committer settings for better reliability
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.speculation", "false")
        
        # Fast upload settings
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
        .config("spark.hadoop.fs.s3a.block.size", "67108864")
        
        # Reduce metadata operations that can cause issues
        .config("spark.hadoop.fs.s3a.metadatastore.impl", 
                "org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore")

        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.executor.cores", "2")
        .config("spark.sql.shuffle.partitions", "8")

        .getOrCreate()
    )
