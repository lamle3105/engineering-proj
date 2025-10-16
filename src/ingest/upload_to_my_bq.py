import pandas as pd
import json
from google.cloud import bigquery
from google.auth import default
import os
from datetime import datetime

# Configuration
PROJECT_ID = "my-project-1561469109609"  # Your accessible project
DATASET_ID = "banking_data"
CARDS_TABLE = "credit_cards"
USERS_TABLE = "users"

# File paths
CARDS_FILE = "data/downloads/extract/Data-Engineering-Workshop-main/sd254_cards.csv"
USERS_FILE = "data/downloads/extract/Data-Engineering-Workshop-main/sd254_users.json"

def setup_bigquery():
    """Initialize BigQuery client and create dataset if needed"""
    creds, _ = default(scopes=["https://www.googleapis.com/auth/bigquery"])
    client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    
    # Create dataset if it doesn't exist
    dataset_ref = client.dataset(DATASET_ID)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {DATASET_ID} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"Created dataset {PROJECT_ID}.{DATASET_ID}")

    return client

def upload_cards_data(client):
    """Upload credit cards CSV to BigQuery"""
    print("Reading credit cards CSV...")
    df = pd.read_csv(CARDS_FILE)
    print(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    print("Columns:", list(df.columns))
    
    # Prepare for upload
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{CARDS_TABLE}"
    
    # Configure job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Replace existing data
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header
        autodetect=True,  # Auto-detect schema
    )
    
    # Upload
    print(f"Uploading to {table_id}...")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for job to complete
    
    table = client.get_table(table_id)
    print(f"Successfully uploaded {table.num_rows} rows to {table_id}")

def upload_users_data(client):
    """Upload users JSON to BigQuery"""
    print("Reading users JSON...")
    with open(USERS_FILE, 'r') as f:
        users_data = json.load(f)
    
    df = pd.json_normalize(users_data)
    print(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    print("Columns:", list(df.columns))
    
    # Prepare for upload
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{USERS_TABLE}"
    
    # Configure job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Replace existing data
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,  # Auto-detect schema
    )
    
    # Upload
    print(f"Uploading to {table_id}...")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for job to complete
    
    table = client.get_table(table_id)
    print(f"Successfully uploaded {table.num_rows} rows to {table_id}")

def main():
    print("Starting BigQuery upload process...")
    print(f"Project: {PROJECT_ID}")
    print(f"Dataset: {DATASET_ID}")
    
    # Setup BigQuery
    client = setup_bigquery()
    
    # Upload both datasets
    upload_cards_data(client)
    upload_users_data(client)
    
    print("\nUpload complete!")
    print(f"You can now query your data using:")
    print(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{CARDS_TABLE}` LIMIT 10")
    print(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{USERS_TABLE}` LIMIT 10")

if __name__ == "__main__":
    main()
