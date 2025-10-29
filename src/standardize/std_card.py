import sys, os
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

from datetime import date
from pyspark.sql import functions as F, Window, types as T
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

# ---------- Helpers ----------
cols_ci = {c.lower(): c for c in df.columns}
def has(name: str) -> bool: return name.lower() in cols_ci
def col(name: str):          return F.col(cols_ci[name.lower()])

# Trim all string columns (non-destructive)
string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, T.StringType)]
for c in string_cols:
    df = df.withColumn(c, F.trim(F.col(c)))

# Safe numeric cast for currency-like strings (remove $ , spaces)
def to_decimal(expr, scale=2):
    cleaned = F.regexp_replace(expr.cast("string"), r"[^0-9\.\-]", "")
    return cleaned.cast(T.DecimalType(18, scale))

# Parse Expires "MM/YY" or "MM/YYYY"
def parse_expiry(col_expr):
    digits = F.regexp_replace(col_expr.cast("string"), r"[^\d/]", "")
    mm = F.regexp_extract(digits, r"^(\d{1,2})", 1).cast("int")
    yy = F.regexp_extract(digits, r"/(\d{2,4})$", 1)
    yyyy = F.when(F.length(yy) == 2, (F.lit(2000) + yy.cast("int"))).otherwise(yy.cast("int"))
    return mm, yyyy

# Boolean normalizer: yes/no, true/false, y/n, 1/0
def to_bool(expr):
    s = F.lower(F.coalesce(expr.cast("string"), F.lit("")))
    return F.when(s.rlike(r"^(y|yes|true|1)$"), F.lit(True)) \
            .when(s.rlike(r"^(n|no|false|0)$"), F.lit(False)) \
            .otherwise(F.lit(None).cast("boolean"))

# ---------- Standardized columns (keep ALL originals) ----------
# IDs / Categorical
if has("User"):
    df = df.withColumn("user_id", col("User"))
if has("CARD INDEX"):
    df = df.withColumn("card_id", col("CARD INDEX"))
if has("Card Brand"):
    df = df.withColumn("card_provider", F.initcap(col("Card Brand")))
if has("Card Type"):
    # normalize a bit (e.g., "credit"/"debit"/"prepaid")
    ct = F.lower(col("Card Type"))
    df = df.withColumn(
        "card_type",
        F.when(ct.startswith("cred"), "CREDIT")
         .when(ct.startswith("deb"), "DEBIT")
         .when(ct.startswith("pre"), "PREPAID")
         .otherwise(F.upper(F.col("Card Type")))
    )

# Card number: digits only + helpers
if has("Card Number"):
    digits = F.regexp_replace(col("Card Number").cast("string"), r"\D", "")
    df = df.withColumn("card_number", digits)
    df = df.withColumn("card_last4", F.when(F.length(digits) >= 4, F.substring(digits, -4, 4)))
    df = df.withColumn("card_bin6",  F.when(F.length(digits) >= 6, F.substring(digits, 1, 6)))

# CVV
if has("CVV"):
    cvv_digits = F.regexp_replace(col("CVV").cast("string"), r"\D", "")
    df = df.withColumn("cvv_digits", cvv_digits)
    df = df.withColumn("cvv_len", F.length(cvv_digits).cast("int"))

# Expires
if has("Expires"):
    mm, yyyy = parse_expiry(col("Expires"))
    df = df.withColumn("expires_month", mm)
    df = df.withColumn("expires_year", yyyy)

# Has Chip
if has("Has Chip"):
    df = df.withColumn("has_chip_bool", to_bool(col("Has Chip")))

# Cards Issued
if has("Cards Issued"):
    df = df.withColumn("cards_issued_int", F.regexp_replace(col("Cards Issued").cast("string"), r"\D", "").cast("int"))

# Credit Limit (money)
if has("Credit Limit"):
    df = df.withColumn("credit_limit_amount", to_decimal(col("Credit Limit"), scale=2))

# Account Open Date → created_at
if has("Acct Open Date"):
    # Normalize to string and replace '-' with '/'
    raw_date = F.regexp_replace(col("Acct Open Date").cast("string"), "-", "/")

    # Extract month and year parts (works for '03/2019', '11/22', etc.)
    month_part = F.regexp_extract(raw_date, r"^(\d{1,2})", 1).cast("int")
    year_part  = F.regexp_extract(raw_date, r"/(\d{2,4})$", 1)
    year_part  = F.when(F.length(year_part) == 2, (F.lit(2000) + year_part.cast("int"))).otherwise(year_part.cast("int"))

    # Combine into YYYY-MM string (not full timestamp)
    created_str = F.concat_ws("-", year_part.cast("string"),
                                     F.lpad(month_part.cast("string"), 2, "0"))

    df = (df
        .withColumn("created_at_year", year_part.cast("int"))
        .withColumn("created_at_month", month_part.cast("int"))
        .withColumn("created_at", created_str)
    )
else:
    df = (df
        .withColumn("created_at_year", F.lit(None).cast("int"))
        .withColumn("created_at_month", F.lit(None).cast("int"))
        .withColumn("created_at", F.lit(None).cast("string"))
    )

# Year PIN last Changed
if has("Year PIN last Changed"):
    df = df.withColumn(
        "year_pin_last_changed_int",
        F.regexp_replace(col("Year PIN last Changed").cast("string"), r"\D", "").cast("int")
    )

# Card on Dark Web
if has("Card on Dark Web"):
    df = df.withColumn("card_on_dark_web_bool", to_bool(col("Card on Dark Web")))

# Ensure standardized columns exist even if missing upstream
for c, typ in [
    ("card_provider", "string"),
    ("card_type", "string"),
    ("card_number", "string"),
]:
    if c not in df.columns:
        df = df.withColumn(c, F.lit(None).cast(typ))

# ---------- Dedup: latest per (user_id, card_id) ----------
if all(c in df.columns for c in ["user_id", "card_id"]):
    w = Window.partitionBy("user_id", "card_id").orderBy(F.col("created_at").desc_nulls_last())
    df = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

# ---------- DQ Checks ----------
valid = F.lit(True)
if "card_id" in df.columns:
    valid = valid & F.col("card_id").isNotNull()
if "user_id" in df.columns:
    valid = valid & F.col("user_id").isNotNull()
if "card_number" in df.columns:
    valid = valid & (F.length(F.col("card_number")) >= 4)

# Optional extra checks (not blocking validity; only for dq_errors):
expiry_ok = F.when(
    F.col("expires_month").isNotNull() & F.col("expires_year").isNotNull() &
    (F.col("expires_month").between(1,12)) & (F.col("expires_year") >= 2000),
    True
).otherwise(F.lit(None))

cvv_ok = F.when(F.col("cvv_len").between(3, 4), True).otherwise(F.lit(None))

credit_ok = F.when(F.col("credit_limit_amount").isNotNull(), True).otherwise(F.lit(None))

good_df = df.filter(valid)
bad_df  = df.filter(~valid)

dq_array = F.array(
    F.when(F.lit("card_id" in df.columns) & F.col("card_id").isNull(), F.lit("missing_card_id")),
    F.when(F.lit("user_id" in df.columns) & F.col("user_id").isNull(), F.lit("missing_user_id")),
    F.when(F.lit("card_number" in df.columns) & (F.length(F.col("card_number")) < 4), F.lit("card_number_short")),
    F.when(expiry_ok.isNull() & F.lit(has("Expires")), F.lit("invalid_expiry")),
    F.when(cvv_ok.isNull() & F.lit(has("CVV")), F.lit("invalid_cvv_len")),
    F.when(credit_ok.isNull() & F.lit(has("Credit Limit")), F.lit("credit_limit_not_numeric")),
)
bad_df = bad_df.withColumn("dq_errors", F.array_remove(dq_array, None))

# Minimal fills for standardized-only (do NOT touch original columns)
fills = {}
if "card_provider" in good_df.columns: fills["card_provider"] = "UNKNOWN"
if "card_type"     in good_df.columns: fills["card_type"]     = "UNKNOWN"
if fills:
    good_df = good_df.fillna(fills)

# ---------- Write outputs (keep ALL columns) ----------
keep_cols = [
    # IDs
    "user_id", "card_id",
    # Card attributes
    "card_provider", "card_type", "card_number", "card_last4", "card_bin6",
    # Dates
    "created_at", "created_at_year", "created_at_month",
    # Numeric / financials
    "credit_limit_amount", "cards_issued_int",
    # Flags
    "has_chip_bool", "card_on_dark_web_bool",
    # CVV + expiry (non-sensitive derivatives only)
    "cvv_digits", "cvv_len", "expires_month", "expires_year",
    # System fields
    "year_pin_last_changed_int"
]

# Keep only those that actually exist
keep_cols = [c for c in keep_cols if c in df.columns]

good_df.select(*keep_cols).write.mode("overwrite").parquet(SILVER_PATH)
bad_df.write.mode("overwrite").parquet(QUAR_PATH)


# DQ summary
total, good, bad = df.count(), good_df.count(), bad_df.count()
dq = spark.createDataFrame([(TODAY, total, good, bad)], "etl_date string, total long, good long, bad long")
dq.coalesce(1).write.mode("overwrite").format("json").save(DQ_DIR)

print(f"[OK] standardized/cards → rows={good} (total={total}, bad={bad})")
spark.stop()