import os, datetime, boto3, paramiko
from botocore.client import Config

INGEST_DATE = datetime.date.today().isoformat()

S3_ENDPOINT = "http://localhost:9000"
AK = "minioadmin"; SK = "minioadmin"

RAW_BUCKET = "raw-bucket"
EXT_LAKE_BUCKET = "externallake"

# S3 client (MinIO)
s3 = boto3.client("s3",
                  endpoint_url=S3_ENDPOINT,
                  aws_access_key_id=AK,
                  aws_secret_access_key=SK,
                  config=Config(signature_version="s3v4"),
                  region_name="us-east-1")

# ------- 1) Fetch CSV from SFTP (simulate WinSCP) -------
sftp_host, sftp_port = "localhost", 2222
sftp_user, sftp_pass = "user", "pass"
remote_csv_path = "/upload/sd254_cards.csv"   # or just "upload/sd254_cards.csv"

os.makedirs("tmp", exist_ok=True)
local_csv = "tmp/sd254_cards.csv"

transport = paramiko.Transport((sftp_host, sftp_port))
transport.connect(username=sftp_user, password=sftp_pass)
sftp = paramiko.SFTPClient.from_transport(transport)
sftp.get(remote_csv_path, local_csv)
sftp.close(); transport.close()

dest_csv_key = f"raw/winscp/cards/ingest_date={INGEST_DATE}/sd254_cards.csv"
s3.upload_file(local_csv, RAW_BUCKET, dest_csv_key)
print("[OK] CSV ->", f"s3://{RAW_BUCKET}/{dest_csv_key}")

# ------- 2) Copy JSON from 'externallake' into raw -------
local_json = "tmp/sd254_users.json"
src_json_key = "sd254_users.json"
dest_json_key = f"raw/lake/users/ingest_date={INGEST_DATE}/sd254_users.json"

s3.download_file(EXT_LAKE_BUCKET, src_json_key, local_json)
s3.upload_file(local_json, RAW_BUCKET, dest_json_key)
print("[OK] JSON ->", f"s3://{RAW_BUCKET}/{dest_json_key}")