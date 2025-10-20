import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date
from src.common.spark_utils import get_spark
from pyspark.sql import functions as F, Window

TODAY = date.today().isoformat()
BUCKET = "raw-bucket" 
RAW_PATH    = f"s3a://{BUCKET}/raw/bq/transactions/ingest_date=2025-10-15/transactions.parquet"
SILVER_PATH = f"s3a://{BUCKET}/standardized/transactions/etl_date={TODAY}"
QUAR_PATH   = f"s3a://{BUCKET}/standardized/quarantine/transactions/etl_date={TODAY}"
DQ_DIR      = f"s3a://{BUCKET}/standardized/_dq/transactions/etl_date={TODAY}"

AMOUNT_MIN, AMOUNT_MAX = 0.01, 1_000_000.0

spark = get_spark("Standardize Transactions")

df = spark.read.parquet(RAW_PATH)

rename_map = {
    "Transaction_ID": "txn_id",
    "UserID": "user_id",
    "CardID": "card_id",
    "Amount": "amount",
    "CreatedAt": "txn_datetime",
    "Merchant_Name": "merchant_name",
    "Merchant_City": "merchant_city",
    "Merchant_State": "merchant_state",
    "Zip": "zip_code",
    "MCC": "mcc",
    "Is_Fraud_": "is_fraud"
}

for old, new in rename_map.items():
    if old in df.columns:
        df = df.withColumnRenamed(old, new)

# ---- Optional columns ----
optional_cols = ["merchant_name","merchant_city","merchant_state","zip_code","mcc","is_fraud"]
for c in optional_cols:
    if c not in df.columns:
        df = df.withColumn(c, F.lit(None).cast("string"))

df = df.withColumn("amount_clean", F.regexp_replace(F.col("amount").cast("string"), r"[^0-9\.\-]", ""))
df = df.withColumn("amount", F.col("amount_clean").cast("float")).drop("amount_clean")

ts_candidates = [c for c in ["txn_datetime","created_at","timestamp","event_time"] if c in df.columns]
if ts_candidates:
    df = df.withColumn("txn_datetime", F.to_timestamp(F.col(ts_candidates[0])))
df = df.withColumn("date", F.to_date("txn_datetime"))

if "is_fraud" in df.columns:
    df = df.withColumn(
        "is_fraud",
        F.when(F.lower(F.col("is_fraud").cast("string")).isin("1","true","t","y","yes"), F.lit(True))
         .when(F.lower(F.col("is_fraud").cast("string")).isin("0","false","f","n","no"), F.lit(False))
         .otherwise(F.col("is_fraud").cast("boolean"))
    )

w = Window.partitionBy("txn_id").orderBy(F.col("txn_datetime").desc_nulls_last())
df = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

valid = (
    F.col("txn_id").isNotNull() &
    F.col("user_id").isNotNull() &
    F.col("card_id").isNotNull() &
    F.col("txn_datetime").isNotNull() &
    F.col("amount").isNotNull() &
    (F.col("amount") >= F.lit(AMOUNT_MIN)) &
    (F.col("amount") <= F.lit(AMOUNT_MAX))
)

good_df = df.filter(valid)
bad_df  = df.filter(~valid)

bad_df = bad_df.withColumn(
    "dq_errors",
    F.array_remove(F.array(
        F.when(F.col("txn_id").isNull(),       F.lit("missing_txn_id")),
        F.when(F.col("user_id").isNull(),      F.lit("missing_user_id")),
        F.when(F.col("card_id").isNull(),      F.lit("missing_card_id")),
        F.when(F.col("txn_datetime").isNull(), F.lit("missing_txn_datetime")),
        F.when(F.col("amount").isNull(),       F.lit("missing_amount")),
        F.when(F.col("amount") < AMOUNT_MIN,   F.lit("amount_too_small")),
        F.when(F.col("amount") > AMOUNT_MAX,   F.lit("amount_too_large")),
    ), F.lit(None))
)

fill_map = {
    "merchant_name":  "UNKNOWN",
    "merchant_city":  "UNKNOWN",
    "merchant_state": "UNK",
    "zip_code":       "00000",
    "mcc":            "0000",
}
for c, v in fill_map.items():
    if c in good_df.columns:
        good_df = good_df.fillna({c: v})

keep_cols = [
    "txn_id","user_id","card_id",
    "amount","txn_datetime","date","is_fraud",
    "merchant_name","merchant_city","merchant_state","zip_code","mcc"
]
good_cols = [c for c in keep_cols if c in good_df.columns]

good_df.select(*good_cols).write.mode("overwrite").parquet(SILVER_PATH)
bad_df.write.mode("overwrite").parquet(QUAR_PATH)

total_rows = df.count()
good_rows  = good_df.count()
bad_rows   = bad_df.count()

dq = spark.createDataFrame(
    [(TODAY, total_rows, good_rows, bad_rows, AMOUNT_MIN, AMOUNT_MAX)],
    schema="etl_date string, total_rows long, good_rows long, bad_rows long, amount_min double, amount_max double"
)
dq.coalesce(1).write.mode("append").format("json").save(DQ_DIR)


print(f"[OK] Standardized → {SILVER_PATH}  rows={good_rows}  (total={total_rows}, bad={bad_rows})")
print(f"[OK] Quarantine   → {QUAR_PATH}")
print(f"[OK] DQ summary   → {DQ_DIR}/part-*.json")

print(f"DQ summary → total={total_rows}, good={good_rows}, bad={bad_rows}, pct_bad={(bad_rows/total_rows)*100:.2f}%")
spark.stop()