# src/common/spark_utils.py
from pyspark.sql import SparkSession

HADOOP_AWS_VER = "3.3.6"
AWS_SDK_BUNDLE_VER = "1.12.691"  

def get_spark(app_name="DataPipeline"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config(
            "spark.jars.packages",
            f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VER},"
            f"com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_BUNDLE_VER}"
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.parquet.output.committer.class",
                "org.apache.parquet.hadoop.ParquetOutputCommitter")
        .getOrCreate()
    )
