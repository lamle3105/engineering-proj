# Data Pipeline Architecture

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                   │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                 │
│  │  PostgreSQL  │  │   REST APIs  │  │  CSV/JSON    │                 │
│  │    MySQL     │  │              │  │   Files      │                 │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                 │
└─────────┼──────────────────┼──────────────────┼──────────────────────────┘
          │                  │                  │
          └──────────────────┼──────────────────┘
                             │
                    ┌────────▼────────┐
                    │  DATA INGESTION │
                    │   (Python)      │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │   AWS S3 DATA   │
                    │      LAKE       │
                    ├─────────────────┤
                    │  RAW LAYER      │ ← Original data
                    ├─────────────────┤
                    │ PROCESSED LAYER │ ← Standardized data
                    ├─────────────────┤
                    │ CURATED LAYER   │ ← Star schema
                    └────────┬────────┘
                             │
          ┌──────────────────┼──────────────────┐
          │                  │                  │
┌─────────▼───────┐ ┌────────▼────────┐ ┌──────▼──────┐
│ SPARK TRANSFORM │ │  DATA MASKING   │ │ DATA QUALITY│
│  & STANDARDIZE  │ │    (PII)        │ │   CHECKS    │
└─────────┬───────┘ └────────┬────────┘ └──────┬──────┘
          │                  │                  │
          └──────────────────┼──────────────────┘
                             │
                    ┌────────▼────────┐
                    │  STAR SCHEMA    │
                    │   BUILDER       │
                    └────────┬────────┘
                             │
          ┌──────────────────┼──────────────────┐
          │                  │                  │
┌─────────▼───────┐ ┌────────▼────────┐ ┌──────▼──────┐
│ dim_customer    │ │  dim_product    │ │  dim_date   │
└─────────────────┘ └─────────────────┘ └─────────────┘
          │                  │                  │
          └──────────────────┼──────────────────┘
                             │
                    ┌────────▼────────┐
                    │   fact_sales    │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │   DATA WAREHOUSE│
                    │ (PostgreSQL/    │
                    │  MySQL)         │
                    └────────┬────────┘
                             │
          ┌──────────────────┼──────────────────┐
          │                  │                  │
┌─────────▼───────┐ ┌────────▼────────┐ ┌──────▼──────┐
│   Power BI      │ │    Tableau      │ │   Custom    │
│                 │ │                 │ │     Apps    │
└─────────────────┘ └─────────────────┘ └─────────────┘

        ORCHESTRATED BY APACHE AIRFLOW
```

## Components

### 1. Data Ingestion Layer

**Purpose**: Extract data from multiple sources and load into S3

**Components**:
- `DatabaseSource`: Connects to PostgreSQL/MySQL databases
- `APISource`: Fetches data from REST APIs
- `FileSource`: Reads CSV, JSON, Parquet files
- `DataIngestionPipeline`: Orchestrates ingestion process

**Flow**:
1. Connect to data source
2. Extract data
3. Validate data quality
4. Add metadata (timestamp, source)
5. Store in S3 raw layer (Parquet format)

### 2. Data Lake (AWS S3)

**Purpose**: Centralized storage for all data

**Layers**:
- **Raw**: Original data as-is (90 days retention)
- **Processed**: Cleaned and standardized data (180 days retention)
- **Curated**: Analytics-ready star schema (365 days retention)

**Storage Format**: Apache Parquet (columnar, compressed)

### 3. Transformation Layer (Apache Spark)

**Purpose**: Process and transform data at scale

**Transformations**:
- Column name standardization
- Data type conversions
- Duplicate removal
- Null handling
- Audit column addition
- Aggregations

**Components**:
- `SparkTransformer`: Base transformation logic
- `DataStandardizer`: Standardization rules
- `StarSchemaBuilder`: Dimensional modeling

### 4. Data Masking

**Purpose**: Protect sensitive PII data

**Techniques**:
- **Hashing**: SHA-256/MD5 for emails
- **Pattern Masking**: Show only last 4 digits (phone, SSN)
- **Tokenization**: Replace with random tokens (credit cards)

**Fields Typically Masked**:
- Email addresses
- Phone numbers
- Social Security Numbers
- Credit card numbers
- Personal addresses

### 5. Star Schema

**Purpose**: Dimensional model optimized for analytics

**Fact Table**:
- `fact_sales`: Transaction-level sales data

**Dimension Tables**:
- `dim_customer`: Customer attributes (SCD Type 2)
- `dim_product`: Product catalog (SCD Type 2)
- `dim_date`: Date attributes for time intelligence
- `dim_location`: Store/location information

**Design Principles**:
- Surrogate keys for all dimensions
- Slowly Changing Dimensions (SCD) support
- Denormalized for query performance
- Indexed foreign keys

### 6. Orchestration (Apache Airflow)

**Purpose**: Schedule and monitor pipeline execution

**DAG Structure**:
```
start 
  → ingest_data 
  → transform_and_standardize 
  → apply_data_masking 
  → build_star_schema 
  → data_quality_checks 
  → end
```

**Schedule**: Daily at 2:00 AM (configurable)

**Features**:
- Retry logic (2 retries with 5-minute delay)
- Email notifications on failure
- Task dependency management
- Execution history tracking

### 7. Business Intelligence Layer

**Purpose**: Enable self-service analytics

**Supported Tools**:
- Power BI (primary)
- Tableau
- Looker
- Custom applications

**Integration**:
- Direct database connection
- Pre-built views for common queries
- Row-level security support
- Real-time or scheduled refresh

## Data Flow

### Daily Pipeline Execution

```
1. 02:00 AM - Airflow DAG triggers
   ↓
2. Ingest data from all sources
   - Database: Full/incremental load
   - APIs: Pull latest data
   - Files: Scan for new files
   ↓
3. Store raw data in S3
   - Format: Parquet
   - Partition: by date and source
   ↓
4. Spark transformation
   - Read from S3 raw layer
   - Apply standardization
   - Write to S3 processed layer
   ↓
5. Data masking
   - Identify PII fields
   - Apply masking rules
   - Write masked data
   ↓
6. Star schema creation
   - Build dimension tables
   - Create fact table
   - Write to S3 curated layer
   ↓
7. Load to data warehouse
   - Truncate/load or upsert
   - Update indexes
   - Refresh materialized views
   ↓
8. Data quality checks
   - Row counts
   - Null checks
   - Referential integrity
   ↓
9. Notify on completion/failure
```

## Technology Stack

### Core Technologies

- **Python 3.8+**: Primary programming language
- **Apache Spark 3.4**: Distributed data processing
- **Apache Airflow 2.7**: Workflow orchestration
- **AWS S3**: Data lake storage
- **PostgreSQL/MySQL**: Data warehouse

### Python Libraries

- **PySpark**: Spark Python API
- **Boto3**: AWS SDK
- **Pandas**: Data manipulation
- **SQLAlchemy**: Database ORM
- **PyYAML**: Configuration management

## Scalability Considerations

### Horizontal Scaling

- **Spark Cluster**: Add more worker nodes
- **Airflow**: Use CeleryExecutor with multiple workers
- **S3**: Auto-scales, no configuration needed

### Vertical Scaling

- Increase Spark executor memory
- Add more CPU cores to workers
- Optimize partition sizes

### Performance Optimization

- Partition large datasets by date
- Use columnar storage (Parquet)
- Cache frequently accessed dimensions
- Implement incremental loads
- Use data skipping with metadata

## Security

### Data Security

- **Encryption at Rest**: S3 server-side encryption
- **Encryption in Transit**: HTTPS/TLS
- **Access Control**: IAM roles and policies
- **Data Masking**: PII protection

### Application Security

- **Secrets Management**: Environment variables, AWS Secrets Manager
- **Network Security**: VPC, security groups
- **Authentication**: Database credentials rotation
- **Audit Logging**: CloudWatch, Airflow logs

## Monitoring

### Metrics

- Pipeline execution time
- Data volume processed
- Error rates
- Data quality scores
- S3 storage costs

### Alerting

- Failed pipeline runs
- Data quality violations
- Long-running jobs
- Resource utilization thresholds

### Tools

- Airflow UI for workflow monitoring
- CloudWatch for AWS resources
- Custom dashboards for business metrics

## Disaster Recovery

### Backup Strategy

- **S3 versioning**: Enabled on all buckets
- **Database backups**: Daily automated backups
- **Configuration**: Version controlled in Git

### Recovery Plan

1. Identify failure point
2. Check backups availability
3. Restore from last good state
4. Rerun failed pipeline steps
5. Validate data integrity

## Future Enhancements

1. **Real-time Streaming**: Kafka integration
2. **ML Pipeline**: Model training and deployment
3. **Data Catalog**: Metadata management
4. **Lineage Tracking**: Data provenance
5. **Auto-scaling**: Dynamic resource allocation
