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
        # S3A + MinIO
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")

        # Timeouts/numeric values (no “s”/“h”)
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")        # sec
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")        # ms
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")  # ms
        .config("spark.hadoop.fs.s3a.multipart.purge", "true")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")    # 24h ms
        .config("spark.hadoop.fs.s3a.multipart.purge.interval", "3600000")# 1h ms

        # Optional: keep Spark work dirs out of OneDrive
        .getOrCreate()
    )
