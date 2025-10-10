import boto3, os
from botocore.client import Config

S3_ENDPOINT = "http://localhost:9000"
AK = "minioadmin"; SK = "minioadmin"

s3 = boto3.client("s3",
                  endpoint_url=S3_ENDPOINT,
                  aws_access_key_id=AK,
                  aws_secret_access_key=SK,
                  config=Config(signature_version="s3v4"),
                  region_name="us-east-1")

LOCAL_JSON = "externallake-staging/sd254_users.json"
BUCKET = "externallake"
KEY = "sd254_users.json"

s3.upload_file(LOCAL_JSON, BUCKET, KEY)
print(f"[OK] Uploaded to s3://{BUCKET}/{KEY}")