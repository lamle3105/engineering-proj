# Data Engineering Pipeline

A comprehensive data engineering solution that ingests data from multiple sources, stores it in an AWS S3 data lake, transforms and standardizes it using Apache Spark, and produces a curated star-schema dataset for business intelligence and analytics.

## 🏗️ Architecture Overview

This project implements a complete end-to-end data pipeline with the following components:

```
Data Sources → Ingestion → S3 Data Lake → Transformation → Star Schema → BI Tools
                              (Raw)      (Spark)      (Curated)   (Power BI)
                                ↓
                          Data Masking
```

### Key Features

- **Multi-Source Data Ingestion**: Support for databases (PostgreSQL, MySQL), REST APIs, and file sources (CSV, JSON, Parquet)
- **AWS S3 Data Lake**: Three-tier data lake architecture (Raw, Processed, Curated)
- **Apache Spark Transformation**: Scalable data transformation and standardization
- **Star Schema Data Warehouse**: Optimized dimensional modeling for analytics
- **Data Masking**: PII protection with multiple masking strategies (hashing, tokenization, pattern masking)
- **Apache Airflow Orchestration**: Automated scheduling and workflow management
- **BI Tool Integration**: Ready for Power BI, Tableau, and other analytics platforms

## 📁 Project Structure

```
engineering-proj/
├── data_pipeline/
│   ├── ingestion/          # Data ingestion modules
│   │   ├── base.py         # Base ingestion classes
│   │   ├── database_source.py  # Database connectors
│   │   ├── api_source.py   # API data source
│   │   └── file_source.py  # File-based sources
│   ├── transformation/     # Spark transformation modules
│   │   ├── spark_transformer.py  # Base transformer
│   │   └── star_schema.py  # Star schema builder
│   ├── masking/           # Data masking
│   │   └── data_masker.py  # Masking utilities
│   └── utils/             # Utility modules
│       ├── s3_utils.py     # S3 operations
│       └── config_loader.py # Configuration management
├── airflow/
│   └── dags/
│       └── data_pipeline_dag.py  # Airflow orchestration
├── config/
│   └── pipeline_config.yaml  # Pipeline configuration
├── sql/
│   ├── create_star_schema.sql   # DDL for star schema
│   └── analytical_queries.sql   # Sample queries
├── scripts/
│   ├── setup_pipeline.py        # Setup script
│   └── generate_sample_data.py  # Sample data generator
├── data/                  # Local data storage
│   ├── raw/
│   ├── processed/
│   └── curated/
├── requirements.txt       # Python dependencies
├── .env.example          # Environment variables template
└── README.md
```

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- Apache Spark 3.4+
- Apache Airflow 2.7+
- AWS Account with S3 access
- PostgreSQL/MySQL (optional, for database sources)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/lamle3105/engineering-proj.git
   cd engineering-proj
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your AWS credentials and configuration
   ```

5. **Setup the data pipeline**
   ```bash
   python scripts/setup_pipeline.py
   ```

### Configuration

Edit `config/pipeline_config.yaml` to configure:
- Data sources (databases, APIs, files)
- S3 bucket and data lake structure
- Spark settings
- Data masking rules
- Airflow scheduling

Edit `.env` file with your credentials:
```env
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
S3_BUCKET_NAME=your-data-lake-bucket
```

## 📊 Data Pipeline Components

### 1. Data Ingestion

The pipeline supports multiple data sources:

**Database Sources**
```python
from data_pipeline.ingestion.database_source import DatabaseSource

config = {
    'name': 'sales_db',
    'type': 'postgresql',
    'connection': {
        'host': 'localhost',
        'port': 5432,
        'database': 'sales',
        'user': 'admin',
        'password': 'password'
    },
    'tables': ['sales_transactions', 'customers']
}

source = DatabaseSource(config)
```

**API Sources**
```python
from data_pipeline.ingestion.api_source import APISource

config = {
    'name': 'inventory_api',
    'type': 'api',
    'endpoint': 'https://api.example.com/inventory',
    'auth_type': 'bearer',
    'api_key': 'your_api_key'
}

source = APISource(config)
```

### 2. Data Transformation with Spark

```python
from data_pipeline.transformation.spark_transformer import DataStandardizer

spark_config = {
    'app_name': 'DataPipeline',
    'master': 'local[*]'
}

standardizer = DataStandardizer(spark_config)
df = standardizer.read_from_s3('s3://bucket/raw/')
standardized_df = standardizer.standardize(df)
```

### 3. Star Schema

The pipeline creates a dimensional model with:
- **Fact Table**: `fact_sales`
- **Dimension Tables**: 
  - `dim_customer`
  - `dim_product`
  - `dim_date`
  - `dim_location`

### 4. Data Masking

Configure masking in `pipeline_config.yaml`:

```yaml
data_masking:
  enabled: true
  fields:
    email:
      method: hash
      algorithm: sha256
    phone:
      method: mask
      pattern: "XXX-XXX-####"
    ssn:
      method: mask
      pattern: "XXX-XX-####"
    credit_card:
      method: tokenize
```

## 🔄 Running the Pipeline

### Manual Execution

1. **Generate sample data** (optional)
   ```bash
   python scripts/generate_sample_data.py
   ```

2. **Run the pipeline** (via individual components or full DAG)

### Airflow Orchestration

1. **Initialize Airflow**
   ```bash
   export AIRFLOW_HOME=$(pwd)/airflow
   airflow db init
   ```

2. **Start Airflow**
   ```bash
   airflow webserver --port 8080
   airflow scheduler
   ```

3. **Access Airflow UI**
   - Navigate to http://localhost:8080
   - Enable the `data_pipeline_dag`
   - The DAG runs daily at 2 AM by default

### DAG Workflow

```
Start → Ingest Data → Transform & Standardize → Apply Data Masking → Build Star Schema → Data Quality Checks → End
```

## 📈 Business Intelligence Integration

### Power BI Connection

1. Use the Direct Query or Import mode
2. Connect to your data warehouse (PostgreSQL/MySQL)
3. Use the views created in `sql/create_star_schema.sql`
4. Import the star schema tables

### Sample Queries

See `sql/analytical_queries.sql` for example queries:
- Sales by product category
- Monthly sales trends
- Top customers by revenue
- Regional performance analysis

## 🔒 Data Security

### Data Masking Features

- **Hashing**: SHA-256/MD5 for irreversible masking
- **Pattern Masking**: Show partial data (e.g., last 4 digits of phone)
- **Tokenization**: Replace sensitive values with tokens
- **Email Masking**: Mask username while keeping domain

### Best Practices

- Store credentials in `.env` (never commit to git)
- Use IAM roles for AWS access when possible
- Enable S3 encryption at rest
- Implement row-level security in your BI tool
- Regularly audit access logs

## 🧪 Testing

Generate sample data for testing:
```bash
python scripts/generate_sample_data.py
```

This creates:
- 1,000 customer records
- 200 product records
- 50 location records
- 10,000 transaction records

## 📝 Data Lake Structure

```
s3://your-bucket/
├── raw/                 # Raw ingested data (90 days retention)
│   ├── database/
│   ├── api/
│   └── files/
├── processed/          # Standardized data (180 days retention)
│   └── masked/        # Masked sensitive data
└── curated/           # Star schema tables (365 days retention)
    ├── dim_customer/
    ├── dim_product/
    ├── dim_date/
    ├── dim_location/
    └── fact_sales/
```

## 🛠️ Customization

### Adding New Data Sources

1. Create a new source class inheriting from `DataSource`
2. Implement `extract()` and `validate()` methods
3. Add configuration to `pipeline_config.yaml`

### Adding New Transformations

1. Create transformation functions in `transformation/` directory
2. Update the Airflow DAG to include new tasks
3. Test transformations with sample data

## 📦 Dependencies

Major dependencies:
- **PySpark**: Distributed data processing
- **Apache Airflow**: Workflow orchestration
- **Boto3**: AWS SDK for Python
- **Pandas**: Data manipulation
- **SQLAlchemy**: Database connectivity

See `requirements.txt` for complete list.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 👥 Authors

- Your Team Name

## 📞 Support

For issues and questions:
- Create an issue in the GitHub repository
- Contact the data engineering team

## 🔮 Future Enhancements

- [ ] Add support for real-time streaming with Kafka
- [ ] Implement data lineage tracking
- [ ] Add data quality monitoring with Great Expectations
- [ ] Support for additional data sources (MongoDB, Snowflake)
- [ ] Machine learning model integration
- [ ] Advanced data profiling and cataloging