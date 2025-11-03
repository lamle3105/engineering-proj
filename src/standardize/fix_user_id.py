import sys, os
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

from pyspark.sql import functions as F, types as T
from src.common.spark_utils import get_spark

BUCKET = "raw-bucket"
RAW_PATH   = f"s3a://{BUCKET}/raw/lake/users/ingest_date=2025-10-10/sd254_users.json"
FIXED_PATH = f"s3a://{BUCKET}/raw/lake/users/ingest_date=2025-10-10/sd254_users_fixed.json"

spark = get_spark("Fix_UserID")
spark.sparkContext.setLogLevel("WARN")

def read_json_robust(path):
    df = (
        spark.read
        .option("mode", "PERMISSIVE")
        .option("multiLine", True)
        .json(path)
    )
    if len(df.columns) == 0:
        df = (
            spark.read
            .option("mode", "PERMISSIVE")
            .option("multiLine", False)
            .json(path)
        )
    return df

df = read_json_robust(RAW_PATH)

rdd = df.rdd.zipWithIndex().map(lambda x: {**x[0].asDict(recursive=False), "user_id": x[1]})
df_fixed = spark.createDataFrame(rdd, schema=df.schema.add("user_id", T.LongType()))

df_fixed.coalesce(1).write.mode("overwrite").json(FIXED_PATH)

print(f"[OK] Added sequential user_id for {df_fixed.count()} rows")
print(f"[OUT] {FIXED_PATH}")

spark.stop()