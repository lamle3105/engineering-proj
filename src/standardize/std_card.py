import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date
from pyspark.sql import functions as F, Window
from src.common.spark_utils import get_spark

TODAY = date.today().isoformat()
BUCKET = "raw-bucket"

RAW_PATH    = f"s3a://{BUCKET}/raw/winscp/cards/ingest_date=2025-10-10/sd254_cards.csv"
SILVER_PATH = f"s3a://{BUCKET}/standardized/cards/etl_date={TODAY}"
QUAR_PATH   = f"s3a://{BUCKET}/standardized/quarantine/cards/etl_date={TODAY}"
DQ_DIR      = f"s3a://{BUCKET}/standardized/_dq/cards/etl_date={TODAY}"

spark = get_spark("Standardize_Cards")
spark.sparkContext.setLogLevel("WARN")

df = (spark.read
      .option("header", True)
      .option("inferSchema", True)
      .csv(RAW_PATH))

rename_map = {
    "cardId": "card_id",
    "userId": "user_id",
    "cardNumber": "card_number",
    "provider": "card_provider",
    "createdAt": "created_at",
    "status": "status",
}
for old, new in rename_map.items():
    if old in df.columns:
        df = df.withColumnRenamed(old, new)

for c in ["card_provider","card_number","status","created_at"]:
    if c not in df.columns:
        df = df.withColumn(c, F.lit(None).cast("string"))

df = df.withColumn("created_at", F.to_timestamp("created_at"))

if "card_number" in df.columns:
    df = df.withColumn("card_number", F.regexp_replace(F.col("card_number").cast("string"), r"\D", ""))

w = Window.partitionBy("card_id").orderBy(F.col("created_at").desc_nulls_last())
df = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn")==1).drop("rn")

valid = (
    F.col("card_id").isNotNull() &
    F.col("user_id").isNotNull() &
    (F.length(F.col("card_number")) >= 4)  # at least last4 exists
)
good_df = df.filter(valid)
bad_df  = df.filter(~valid).withColumn(
    "dq_errors",
    F.array_remove(F.array(
        F.when(F.col("card_id").isNull(),      F.lit("missing_card_id")),
        F.when(F.col("user_id").isNull(),      F.lit("missing_user_id")),
        F.when(F.length(F.col("card_number")) < 4, F.lit("card_number_short")),
    ), F.lit(None))
)

fills = {"card_provider":"UNKNOWN","status":"UNKNOWN"}
good_df = good_df.fillna(fills)

keep_cols = ["card_id","user_id","card_provider","card_number","status","created_at"]
good_cols = [c for c in keep_cols if c in good_df.columns]

good_df.select(*good_cols).write.mode("overwrite").parquet(SILVER_PATH)
bad_df.write.mode("overwrite").parquet(QUAR_PATH)

total, good, bad = df.count(), good_df.count(), bad_df.count()
dq = spark.createDataFrame([(TODAY,total,good,bad)], "etl_date string, total long, good long, bad long")
(dq.coalesce(1).write.mode("overwrite").format("json").save(DQ_DIR))

print(f"[OK] standardized/cards → rows={good} (total={total}, bad={bad})")
spark.stop()
