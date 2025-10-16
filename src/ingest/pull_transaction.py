import io, os, argparse, pandas as pd
from datetime import date
from google.cloud import bigquery
from google.auth import default
import boto3
from botocore.client import Config

# ---- CLI args ----
ap = argparse.ArgumentParser()
ap.add_argument("--project", default=os.getenv("GCP_PROJECT_ID"))
ap.add_argument("--table", help="Fully-qualified table: project.dataset.table")
ap.add_argument("--query", help="Custom SQL instead of --table")
ap.add_argument("--limit", type=int, default=0, help="Optional row limit")
ap.add_argument("--raw-prefix", default="raw/bq/transactions")
args = ap.parse_args()

# ---- BigQuery (user ADC) ----
creds, default_project = default(scopes=["https://www.googleapis.com/auth/bigquery"])
project = args.project or default_project
bq = bigquery.Client(project=project, credentials=creds)

# ---- S3/MinIO ----
S3_ENDPOINT = "http://localhost:9000"
AK = "minioadmin"; SK = "minioadmin"
BUCKET = "raw-bucket"
TODAY = date.today().isoformat()

s3 = boto3.client("s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AK,
    aws_secret_access_key=SK,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

# ---- Build query ----
if args.query:
    sql = args.query.strip().rstrip(";")
else:
    if not args.table:
        raise SystemExit("Provide --table project.dataset.table or --query SQL")
    sql = f"SELECT * FROM `{args.table}`"
if args.limit and args.limit > 0:
    sql += f"\nLIMIT {args.limit}"

print("Running SQL:\n", sql)
df = bq.query(sql).result().to_dataframe(create_bqstorage_client=False)
print(f"Rows: {len(df)}  Cols: {list(df.columns)}")

# ---- Write Parquet to raw ----
if df.empty:
    print("[WARN] Query returned 0 rows.")
else:
    buf = io.BytesIO(); df.to_parquet(buf, index=False); buf.seek(0)
    key = f"{args.raw_prefix}/ingest_date={TODAY}/transactions.parquet"
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    print(f"[OK] s3://{BUCKET}/{key}")