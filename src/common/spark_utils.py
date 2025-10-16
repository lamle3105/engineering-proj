from pyspark.sql import SparkSession

def get_spark_session(app_name="Data ETL Pipeline"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.parquet.output.committer.class", 
                "org.apache.parquet.hadoop.ParquetOutputCommitter")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark