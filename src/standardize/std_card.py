import sys, os
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

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

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(RAW_PATH)
)

# ---- Case-insensitive column helpers ----
cols_ci = {c.lower(): c for c in df.columns}
def has(name: str) -> bool: return name.lower() in cols_ci
def col(name: str):          return F.col(cols_ci[name.lower()])

# ---- Map actual headers to standardized names ----
# Current CSV headers: User, CARD INDEX, Card Brand, Card Type, Card Number, Acct Open Date, ...
selects = []
if has("User"):           selects.append(col("User").alias("user_id"))
if has("CARD INDEX"):     selects.append(col("CARD INDEX").alias("card_id"))
if has("Card Brand"):     selects.append(col("Card Brand").alias("card_provider"))
if has("Card Type"):      selects.append(col("Card Type").alias("status"))  # or "card_type"
if has("Card Number"):    selects.append(col("Card Number").cast("string").alias("card_number"))
if has("Acct Open Date"): selects.append(col("Acct Open Date").alias("created_at_raw"))

# Also support the old camelCase schema if it ever appears
legacy_map = {
    "cardId": "card_id",
    "userId": "user_id",
    "cardNumber": "card_number",
    "provider": "card_provider",
    "createdAt": "created_at_raw",
    "status": "status",
}
for src, dst in legacy_map.items():
    if has(src) and dst not in [a._jc.toString().split(" AS ")[-1] for a in selects]:
        selects.append(col(src).alias(dst))

df = df.select(*selects)

# ---- Normalize values ----
# Clean card_number to digits only
if "card_number" in df.columns:
    df = df.withColumn("card_number", F.regexp_replace(F.col("card_number"), r"\D", ""))

# Parse created_at from multiple common formats
if "created_at_raw" in df.columns:
    created_parsed = F.coalesce(
        F.to_timestamp("created_at_raw", "yyyy-MM-dd"),
        F.to_timestamp("created_at_raw", "dd/MM/yyyy"),
        F.to_timestamp("created_at_raw", "MM/dd/yyyy"),
        F.to_timestamp("created_at_raw"),  # fallback
    )
    df = df.withColumn("created_at", created_parsed).drop("created_at_raw")
else:
    df = df.withColumn("created_at", F.lit(None).cast("timestamp"))

# Ensure optional columns exist
for c in ["card_provider", "status"]:
    if c not in df.columns:
        df = df.withColumn(c, F.lit(None).cast("string"))

# ---- Deduplicate: latest per (user_id, card_id) ----
if all(c in df.columns for c in ["user_id", "card_id"]):
    w = Window.partitionBy("user_id", "card_id").orderBy(F.col("created_at").desc_nulls_last())
    df = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

# ---- DQ rules ----
valid = F.lit(True)
if "card_id" in df.columns:
    valid = valid & F.col("card_id").isNotNull()
if "user_id" in df.columns:
    valid = valid & F.col("user_id").isNotNull()
if "card_number" in df.columns:
    valid = valid & (F.length(F.col("card_number")) >= 4)

good_df = df.filter(valid)
bad_df  = df.filter(~valid)

bad_df = bad_df.withColumn(
    "dq_errors",
    F.array_remove(
        F.array(
            F.when(F.lit("card_id" in df.columns) & F.col("card_id").isNull(), F.lit("missing_card_id")),
            F.when(F.lit("user_id" in df.columns) & F.col("user_id").isNull(), F.lit("missing_user_id")),
            F.when(F.lit("card_number" in df.columns) & (F.length(F.col("card_number")) < 4), F.lit("card_number_short")),
        ),
        F.lit(None),
    ),
)

# Fills
fills = {"card_provider": "UNKNOWN", "status": "UNKNOWN"}
good_df = good_df.fillna(fills)

# ---- Write outputs ----
keep_cols = [c for c in ["card_id","user_id","card_provider","card_number","status","created_at"] if c in good_df.columns]
good_df.select(*keep_cols).write.mode("overwrite").parquet(SILVER_PATH)
bad_df.write.mode("overwrite").parquet(QUAR_PATH)

# DQ summary
total, good, bad = df.count(), good_df.count(), bad_df.count()
dq = spark.createDataFrame([(TODAY, total, good, bad)], "etl_date string, total long, good long, bad long")
dq.coalesce(1).write.mode("overwrite").format("json").save(DQ_DIR)

print(f"[OK] standardized/cards → rows={good} (total={total}, bad={bad})")
spark.stop()