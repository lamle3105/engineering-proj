# probe_s3a.py
from src.common.spark_utils import get_spark

spark = get_spark("probe")

# try reading a specific file instead of listing the bucket
try:
    df = spark.read.option("header", True).csv("s3a://raw-bucket/raw/winscp/cards/ingest_date=2025-10-10/sd254_cards.csv")
    print("S3A OK, count:", df.count())
    print("Columns:", df.columns)
except Exception as e:
    print("Error:", str(e))

spark.stop()
