import sys, os
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

from datetime import date
from pyspark.sql import functions as F, types as T
from src.common.spark_utils import get_spark

TODAY = date.today().isoformat()
BUCKET = "raw-bucket"

RAW_PATH    = f"s3a://{BUCKET}/raw/lake/users/ingest_date=2025-10-10/sd254_users_fixed.json"
SILVER_PATH = f"s3a://{BUCKET}/standardized/sor/users/etl_date={TODAY}"
QUAR_PATH   = f"s3a://{BUCKET}/standardized/quarantine/users/etl_date={TODAY}"
DQ_DIR      = f"s3a://{BUCKET}/standardized/_dq/users/etl_date={TODAY}"

spark = get_spark("Standardize_Users")
spark.sparkContext.setLogLevel("WARN")

df = spark.read.json(RAW_PATH)

if "user_id" not in df.columns:
    raise RuntimeError("Expected 'user_id' in raw users file, but it is missing.")
df = df.withColumn("user_id", F.col("user_id").cast("long"))

string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, T.StringType)]
for c in string_cols:
    df = df.withColumn(c, F.trim(F.col(c)))

cols_ci = {c.lower(): c for c in df.columns}
def has(name): return name.lower() in cols_ci
def col(name): return F.col(cols_ci[name.lower()])

def to_int(s):   return F.regexp_replace(s.cast("string"), r"[^\-\d]", "").cast("int")
def to_float(s): return F.regexp_replace(s.cast("string"), r"[^\-\d\.]", "").cast("double")
def money_to_decimal(s, scale=2):
    return F.regexp_replace(s.cast("string"), r"[^0-9\.\-]", "").cast(T.DecimalType(18, scale))

def norm_gender(s):
    x = F.lower(F.coalesce(s.cast("string"), F.lit("")))
    return (F.when(x.rlike(r"^(f|female|w|woman)$"), F.lit("FEMALE"))
             .when(x.rlike(r"^(m|male|man)$"), F.lit("MALE"))
             .otherwise(F.lit("UNKNOWN")))

if has("Person"):
    df = df.withColumn("person_name", col("Person"))
    df = df.withColumn("first_name", F.split(F.col("person_name"), r"\s+")[0])
    df = df.withColumn("last_name",  F.element_at(F.split(F.col("person_name"), r"\s+"), -1))
else:
    df = (df.withColumn("person_name", F.lit(None).cast("string"))
            .withColumn("first_name",  F.lit(None).cast("string"))
            .withColumn("last_name",   F.lit(None).cast("string")))

df = (df
    .withColumn("age_current",     to_int(col("Current Age"))      if has("Current Age")      else F.lit(None).cast("int"))
    .withColumn("age_retirement",  to_int(col("Retirement Age"))   if has("Retirement Age")   else F.lit(None).cast("int"))
    .withColumn("birth_year",      to_int(col("Birth Year"))       if has("Birth Year")       else F.lit(None).cast("int"))
    .withColumn("birth_month",     to_int(col("Birth Month"))      if has("Birth Month")      else F.lit(None).cast("int"))
    .withColumn("gender",          norm_gender(col("Gender"))      if has("Gender")           else F.lit("UNKNOWN"))
)

df = (df
    .withColumn("address_line1", col("Address") if has("Address") else F.lit(None).cast("string"))
    .withColumn("city",          col("City")    if has("City")    else F.lit(None).cast("string"))
    .withColumn("state",         col("State")   if has("State")   else F.lit(None).cast("string"))
    .withColumn("zipcode",       F.regexp_replace(col("Zipcode"), r"[^\d]", "") if has("Zipcode") else F.lit(None).cast("string"))
)

df = (df
    .withColumn("latitude",  to_float(col("Latitude"))  if has("Latitude")  else F.lit(None).cast("double"))
    .withColumn("longitude", to_float(col("Longitude")) if has("Longitude") else F.lit(None).cast("double"))
)

df = (df
    .withColumn("per_capita_income_zip", money_to_decimal(col("Per Capita Income - Zipcode")) if has("Per Capita Income - Zipcode") else F.lit(None).cast(T.DecimalType(18,2)))
    .withColumn("income_yearly_person",  money_to_decimal(col("Yearly Income - Person"))      if has("Yearly Income - Person")      else F.lit(None).cast(T.DecimalType(18,2)))
    .withColumn("total_debt",            money_to_decimal(col("Total Debt"))                  if has("Total Debt")                   else F.lit(None).cast(T.DecimalType(18,2)))
    .withColumn("fico_score",            to_int(col("FICO Score"))                            if has("FICO Score")                   else F.lit(None).cast("int"))
    .withColumn("num_credit_cards",      to_int(col("Num Credit Cards"))                      if has("Num Credit Cards")             else F.lit(None).cast("int"))
)

df = df.withColumn(
    "years_to_retirement",
    F.when(F.col("age_retirement").isNotNull() & F.col("age_current").isNotNull(),
           (F.col("age_retirement") - F.col("age_current")).cast("int"))
)

df = df.withColumn(
    "debt_to_income",
    F.when(F.col("income_yearly_person").isNotNull() & (F.col("income_yearly_person") != 0),
           (F.col("total_debt") / F.col("income_yearly_person")).cast(T.DecimalType(18,4)))
)

df = df.dropDuplicates(["user_id"])

valid = F.col("user_id").isNotNull() & F.col("person_name").isNotNull()

dq_array = F.array(
    F.when(~F.col("age_current").between(0, 120) & F.col("age_current").isNotNull(), F.lit("age_current_out_of_range")),
    F.when(~F.col("fico_score").between(300, 850) & F.col("fico_score").isNotNull(), F.lit("fico_out_of_range")),
    F.when(~F.col("latitude").between(-90, 90)    & F.col("latitude").isNotNull(),  F.lit("lat_out_of_range")),
    F.when(~F.col("longitude").between(-180, 180) & F.col("longitude").isNotNull(), F.lit("lon_out_of_range")),
    F.when((F.length(F.col("zipcode")) < 5) & F.col("zipcode").isNotNull(), F.lit("zipcode_short"))
)

bad_df = df.filter(~valid).withColumn("dq_errors", F.array_remove(dq_array, None))
good_df = df.filter(valid).fillna({"gender": "UNKNOWN"})

silver_cols = [
    "user_id",
    "person_name", "first_name", "last_name",
    "gender",
    "age_current", "age_retirement", "years_to_retirement",
    "birth_year", "birth_month",
    "address_line1", "city", "state", "zipcode",
    "latitude", "longitude",
    "per_capita_income_zip", "income_yearly_person", "total_debt",
    "debt_to_income",
    "fico_score", "num_credit_cards",
]
silver_cols = [c for c in silver_cols if c in good_df.columns]
good_df.select(*silver_cols).write.mode("overwrite").parquet(SILVER_PATH)

bad_df.write.mode("overwrite").parquet(QUAR_PATH)

total, good, bad = df.count(), good_df.count(), bad_df.count()
dq = spark.createDataFrame([(TODAY, total, good, bad)],
                           "etl_date string, total long, good long, bad long")
dq.coalesce(1).write.mode("overwrite").format("json").save(DQ_DIR)

print(f"[OK] standardized/users → rows={good} (total={total}, bad={bad})")
spark.stop()
