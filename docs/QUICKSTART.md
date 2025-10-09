# Quick Start Guide

This guide will help you get the data pipeline up and running quickly for testing and development.

## Prerequisites

- Python 3.8 or higher
- pip package manager
- AWS account (for production S3 usage)
- Docker (optional, for Airflow)

## Quick Start (5 minutes)

### 1. Install Dependencies

```bash
# Clone the repository
git clone https://github.com/lamle3105/engineering-proj.git
cd engineering-proj

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
# Copy example environment file
cp .env.example .env

# Edit .env with your settings (for local testing, use minimal config)
nano .env
```

**Minimal Configuration for Local Testing:**
```env
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
S3_BUCKET_NAME=test-bucket
SPARK_MASTER=local[*]
MASKING_ENABLED=true
```

### 3. Generate Sample Data

```bash
# Generate sample data for testing
python scripts/generate_sample_data.py
```

This creates:
- 1,000 customer records
- 200 product records
- 50 location records
- 10,000 sales transactions

### 4. Test Individual Components

#### Test Data Ingestion

```python
from data_pipeline.ingestion.file_source import FileSource

config = {
    'name': 'sales_csv',
    'type': 'file',
    'location': 'data/raw/sales_transactions.csv',
    'format': 'csv'
}

source = FileSource(config)
data = source.extract()
print(f"Extracted {len(data)} records")
```

#### Test Data Masking

```python
from data_pipeline.masking.data_masker import DataMasker

# Test email masking
masker = DataMasker({'enabled': True, 'fields': {}})
masked_email = masker.mask_email('john.doe@example.com')
print(f"Masked: {masked_email}")  # Output: j*****e@example.com

# Test phone masking
masked_phone = masker.mask_phone('555-123-4567')
print(f"Masked: {masked_phone}")  # Output: XXX-XXX-4567
```

### 5. Run Spark Transformation (Local Mode)

```python
from data_pipeline.transformation.spark_transformer import DataStandardizer
import pandas as pd

# Create sample data
df = pd.read_csv('data/raw/sales_transactions.csv')

# Initialize Spark
spark_config = {
    'app_name': 'TestPipeline',
    'master': 'local[*]'
}

standardizer = DataStandardizer(spark_config)

# Convert pandas to Spark DataFrame
spark_df = standardizer.spark.createDataFrame(df)

# Standardize
standardized_df = standardizer.standardize(spark_df)
standardized_df.show(5)

standardizer.stop()
```

## Running with Docker (Airflow)

### 1. Create Docker Compose File

Create `docker-compose.yml`:

```yaml
version: '3'
services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports:
      - "5432:5432"
  
  airflow:
    image: apache/airflow:2.7.0
    depends_on:
      - postgres
    environment:
      AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./data_pipeline:/opt/airflow/data_pipeline
      - ./config:/opt/airflow/config
    ports:
      - "8080:8080"
    command: >
      bash -c "airflow db init &&
               airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com &&
               airflow webserver"
```

### 2. Start Services

```bash
docker-compose up -d
```

### 3. Access Airflow

- URL: http://localhost:8080
- Username: admin
- Password: admin

## Testing the Complete Pipeline

### Option 1: Run Pipeline Script

Create `run_pipeline.py`:

```python
#!/usr/bin/env python3
"""
Simple script to run the complete pipeline locally
"""

import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))

from data_pipeline.utils.config_loader import ConfigLoader
from data_pipeline.ingestion.file_source import FileSource
from data_pipeline.transformation.spark_transformer import DataStandardizer
from data_pipeline.masking.data_masker import DataMasker

def run_pipeline():
    print("=" * 60)
    print("Starting Data Pipeline")
    print("=" * 60)
    
    # Load configuration
    config = ConfigLoader()
    
    # Step 1: Ingest data
    print("\n[1/4] Ingesting data from CSV files...")
    file_config = {
        'name': 'sales_transactions',
        'location': 'data/raw/sales_transactions.csv',
        'format': 'csv'
    }
    source = FileSource(file_config)
    data = source.extract()
    print(f"✓ Ingested {len(data)} records")
    
    # Step 2: Transform with Spark
    print("\n[2/4] Transforming data with Spark...")
    spark_config = config.get_spark_config()
    standardizer = DataStandardizer(spark_config)
    
    spark_df = standardizer.spark.createDataFrame(data)
    standardized_df = standardizer.standardize(spark_df)
    print(f"✓ Standardized {standardized_df.count()} records")
    
    # Step 3: Apply data masking
    print("\n[3/4] Applying data masking...")
    masking_config = config.get_masking_config()
    masker = DataMasker(masking_config)
    
    masked_df = masker.apply_masking_policy(standardized_df)
    print(f"✓ Masked sensitive data")
    
    # Step 4: Save results
    print("\n[4/4] Saving results...")
    output_path = f"data/processed/pipeline_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    masked_df.write.mode('overwrite').parquet(output_path)
    print(f"✓ Saved to {output_path}")
    
    standardizer.stop()
    
    print("\n" + "=" * 60)
    print("Pipeline completed successfully!")
    print("=" * 60)

if __name__ == '__main__':
    run_pipeline()
```

Run it:
```bash
python run_pipeline.py
```

### Option 2: Run via Airflow

1. Ensure Docker services are running
2. Navigate to http://localhost:8080
3. Enable the `data_pipeline_dag`
4. Click "Trigger DAG"
5. Monitor execution in the UI

## Verification Steps

### 1. Check Generated Data

```bash
ls -lh data/raw/
# Should show: customers.csv, products.csv, locations.csv, sales_transactions.csv
```

### 2. Verify Transformations

```python
import pandas as pd

# Load original data
original = pd.read_csv('data/raw/sales_transactions.csv')
print(f"Original records: {len(original)}")

# Load processed data
processed = pd.read_parquet('data/processed/pipeline_output_*/')
print(f"Processed records: {len(processed)}")

# Check for new columns
print(f"New columns: {set(processed.columns) - set(original.columns)}")
```

### 3. Test Data Masking

```python
import pandas as pd

# Load customer data
customers = pd.read_csv('data/raw/customers.csv')
print("Original email:", customers.iloc[0]['email'])

# After masking in pipeline
processed = pd.read_parquet('data/processed/pipeline_output_*/')
if 'email' in processed.columns:
    print("Masked email:", processed.iloc[0]['email'])
```

## Troubleshooting

### Issue: Spark Memory Error

**Solution:** Increase Spark memory in `config/pipeline_config.yaml`:
```yaml
spark:
  configs:
    spark.executor.memory: 2g
    spark.driver.memory: 1g
```

### Issue: Import Errors

**Solution:** Ensure you're in the project root and virtual environment is activated:
```bash
source venv/bin/activate
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

### Issue: AWS Credentials

**Solution:** For local testing, you can skip S3 and use local file system:
- Use local paths instead of s3:// paths
- Or use LocalStack for S3 emulation

## Next Steps

1. **Configure Real Data Sources**: Update `config/pipeline_config.yaml` with your actual database connections and API endpoints

2. **Setup AWS S3**: Create an S3 bucket and update credentials in `.env`

3. **Schedule Pipeline**: Configure Airflow schedule in the DAG configuration

4. **Connect BI Tools**: Follow the Power BI integration guide in `docs/POWERBI_INTEGRATION.md`

5. **Production Deployment**: 
   - Deploy Airflow on ECS/EKS
   - Use EMR for Spark cluster
   - Setup CloudWatch monitoring

## Resources

- [Main README](../README.md)
- [Architecture Documentation](../docs/ARCHITECTURE.md)
- [Power BI Integration Guide](../docs/POWERBI_INTEGRATION.md)
- [Airflow Documentation](https://airflow.apache.org/docs/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)

## Getting Help

If you encounter issues:
1. Check the logs in `logs/` directory
2. Review Airflow logs in the UI
3. Consult the troubleshooting section in the main README
4. Create an issue on GitHub
