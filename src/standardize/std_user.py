import sys, os
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

from datetime import date
from pyspark.sql import functions as F, Window
from src.common.spark_utils import get_spark

TODAY = date.today().isoformat()
BUCKET = "raw-bucket"

RAW_PATH    = f"s3a://{BUCKET}/raw/lake/users/ingest_date=2025-10-10/sd254_users.json"
SILVER_PATH = f"s3a://{BUCKET}/standardized/users/etl_date={TODAY}"
QUAR_PATH   = f"s3a://{BUCKET}/standardized/quarantine/users/etl_date={TODAY}"
DQ_DIR      = f"s3a://{BUCKET}/standardized/_dq/users/etl_date={TODAY}"

spark = get_spark("Standardize_Users")
spark.sparkContext.setLogLevel("WARN")

df = spark.read.json(RAW_PATH)

rename_map = {
    "userId": "user_id",
    "name": "user_name",
    "email": "email",
    "createdAt": "created_at",
    "updatedAt": "updated_at",
}
for old, new in rename_map.items():
    if old in df.columns:
        df = df.withColumnRenamed(old, new)

for c in ["user_name","email","created_at","updated_at"]:
    if c not in df.columns:
        df = df.withColumn(c, F.lit(None).cast("string"))

df = df.withColumn("created_at", F.to_timestamp("created_at"))
df = df.withColumn("updated_at", F.to_timestamp("updated_at"))

email_ok = F.col("email").isNotNull() & F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
valid = F.col("user_id").isNotNull() & email_ok

w = Window.partitionBy("user_id").orderBy(F.col("updated_at").desc_nulls_last(), F.col("created_at").desc_nulls_last())
df = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn")==1).drop("rn")

good_df = df.filter(valid)
bad_df  = df.filter(~valid).withColumn(
    "dq_errors",
    F.array_remove(F.array(
        F.when(F.col("user_id").isNull(), F.lit("missing_user_id")),
        F.when(F.col("email").isNull(),   F.lit("missing_email")),
        F.when(~F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"), F.lit("invalid_email")),
    ), F.lit(None))
)

good_df = good_df.fillna({"user_name":"UNKNOWN"})

keep_cols = ["user_id","user_name","email","created_at","updated_at"]
good_cols = [c for c in keep_cols if c in good_df.columns]

good_df.select(*good_cols).write.mode("overwrite").parquet(SILVER_PATH)
bad_df.write.mode("overwrite").parquet(QUAR_PATH)

total, good, bad = df.count(), good_df.count(), bad_df.count()
dq = spark.createDataFrame([(TODAY,total,good,bad)], "etl_date string, total long, good long, bad long")
(dq.coalesce(1).write.mode("overwrite").format("json").save(DQ_DIR))

print(f"[OK] standardized/users → rows={good} (total={total}, bad={bad})")
spark.stop()
