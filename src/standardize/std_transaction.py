import sys, os
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

from datetime import date
from pyspark.sql import functions as F, types as T
from src.common.spark_utils import get_spark

TODAY = date.today().isoformat()
BUCKET = "raw-bucket"

RAW_PATH    = f"s3a://{BUCKET}/raw/bq/transactions/ingest_date=2025-10-15/transactions.parquet"
SILVER_PATH = f"s3a://{BUCKET}/standardized/sor/transactions/etl_date={TODAY}"
QUAR_PATH   = f"s3a://{BUCKET}/standardized/quarantine/transactions/etl_date={TODAY}"
DQ_DIR      = f"s3a://{BUCKET}/standardized/_dq/transactions/etl_date={TODAY}"

spark = get_spark("Standardize_Transactions")
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(RAW_PATH)

cols_ci = {c.lower(): c for c in df.columns}
def has(name): return name.lower() in cols_ci
def col(name): return F.col(cols_ci[name.lower()])

for c, t in df.dtypes:
    if t == "string":
        df = df.withColumn(c, F.trim(F.col(c)))

rename_map = {
    "User": "user_id",
    "Card": "card_id",
    "Year": "txn_year",
    "Month": "txn_month",
    "Day": "txn_day",
    "Time": "txn_time",
    "Amount": "amount",
    "Use_Chip": "use_chip",
    "Merchant_Name": "merchant_name",
    "Merchant_City": "merchant_city",
    "Merchant_State": "merchant_state",
    "Zip": "zipcode",
    "MCC": "mcc",
    "Errors_": "errors",
    "Is_Fraud_": "is_fraud",
}
for old, new in rename_map.items():
    if has(old):
        df = df.withColumnRenamed(cols_ci[old.lower()], new)

df = (df
    .withColumn("user_id",   F.col("user_id").cast("long"))
    .withColumn("card_id",   F.col("card_id").cast("long"))
    .withColumn("txn_year",  F.col("txn_year").cast("int"))
    .withColumn("txn_month", F.col("txn_month").cast("int"))
    .withColumn("txn_day",   F.col("txn_day").cast("int"))
    .withColumn("amount",
        F.regexp_replace(F.col("amount").cast("string"), r"[^0-9\.\-]", "").cast(T.DecimalType(18, 2)))
    .withColumn("zipcode",   F.regexp_replace(F.col("zipcode").cast("string"), r"[^\d]", ""))
    .withColumn("merchant_name",  F.col("merchant_name").cast("string"))
    .withColumn("merchant_city",  F.col("merchant_city").cast("string"))
    .withColumn("merchant_state", F.col("merchant_state").cast("string"))
    .withColumn("mcc",            F.col("mcc").cast("string"))
    .withColumn("errors",         F.col("errors").cast("string"))
)

chip_s = F.lower(F.col("use_chip").cast("string"))
df = df.withColumn(
    "use_chip",
    F.when(chip_s.rlike(r"\b(swipe|swiped|magstripe|mag|msr)\b"), F.lit("SWIPE"))
     .when(chip_s.rlike(r"\b(chip|emv|insert)\b"),                F.lit("CHIP"))
     .when(chip_s.rlike(r"\b(online|ecom|not[_\s]?chip|no chip|keyed|card not present)\b"), F.lit("ONLINE"))
     .otherwise(F.lit("UNKNOWN"))
)

day2   = F.lpad(F.col("txn_day").cast("string"), 2, "0")
month2 = F.lpad(F.col("txn_month").cast("string"), 2, "0")
year4  = F.col("txn_year").cast("string")
date_str = F.concat_ws("-", day2, month2, year4)

time_s = F.col("txn_time").cast("string")
hh = F.lpad(F.regexp_extract(time_s, r'^\s*(\d{1,2})', 1), 2, "0")
mm = F.lpad(F.regexp_extract(time_s, r'^\s*\d{1,2}:(\d{2})', 1), 2, "0")
time_str = F.concat_ws(":", hh, mm)

df = df.withColumn(
    "transaction_datetime",
    F.to_timestamp(F.concat_ws(" ", date_str, time_str), "dd-MM-yyyy HH:mm")
)

df = df.withColumn("weekday", F.date_format(F.col("transaction_datetime"), "E"))
df = df.withColumn("hour", F.hour(F.col("transaction_datetime")))

# errors: null/blank => "NONE"
err_s = F.trim(F.col("errors"))
df = df.withColumn("errors", F.when(F.coalesce(err_s, F.lit("")) == "", F.lit("NONE")).otherwise(err_s))

# is_fraud to boolean
fraud_s = F.lower(F.col("is_fraud").cast("string"))
df = df.withColumn(
    "is_fraud",
    F.when(fraud_s.isin("1", "true", "y", "yes", "t"), F.lit(True)).otherwise(F.lit(False))
)

valid = (
    F.col("user_id").isNotNull() &
    F.col("card_id").isNotNull() &
    F.col("amount").isNotNull() & (F.col("amount") != 0) &
    F.col("transaction_datetime").isNotNull()
)

dq_array = F.array(
    F.when(F.col("user_id").isNull(), F.lit("missing_user_id")),
    F.when(F.col("card_id").isNull(), F.lit("missing_card_id")),
    F.when(F.col("amount").isNull() | (F.col("amount") == 0), F.lit("invalid_amount")),
    F.when(F.col("transaction_datetime").isNull(), F.lit("missing_timestamp"))
)
bad_df = df.filter(~valid).withColumn("dq_errors", F.array_remove(dq_array, None))
good_df = df.filter(valid)

good_df = good_df.fillna({
    "merchant_name": "UNKNOWN",
    "merchant_city": "UNKNOWN",
    "merchant_state": "UNKNOWN",
    "mcc": "UNKNOWN",
    "zipcode": ""
})

silver_cols = [
    "user_id", "card_id", "transaction_datetime",
    "txn_year", "txn_month", "txn_day", "hour", "weekday",
    "amount", "use_chip",
    "merchant_name", "merchant_city", "merchant_state",
    "zipcode", "mcc", "is_fraud", "errors"
]
silver_cols = [c for c in silver_cols if c in good_df.columns]

good_df.select(*silver_cols).write.mode("overwrite").parquet(SILVER_PATH)
bad_df.write.mode("overwrite").parquet(QUAR_PATH)

total, good, bad = df.count(), good_df.count(), bad_df.count()
dq = spark.createDataFrame([(TODAY, total, good, bad)],
                           "etl_date string, total long, good long, bad long")
dq.coalesce(1).write.mode("overwrite").format("json").save(DQ_DIR)

print(f"[OK] standardized/transactions -> rows={good} (total={total}, bad={bad})")
spark.stop()
