#!/usr/bin/env python3
"""
Validate project structure and basic configuration
"""

import os
import sys


def check_file_exists(path, description):
    """Check if a file exists"""
    if os.path.exists(path):
        print(f"✓ {description}: {path}")
        return True
    else:
        print(f"✗ {description}: {path} - NOT FOUND")
        return False


def check_directory_exists(path, description):
    """Check if a directory exists"""
    if os.path.isdir(path):
        print(f"✓ {description}: {path}")
        return True
    else:
        print(f"✗ {description}: {path} - NOT FOUND")
        return False


def validate_project_structure():
    """Validate the project structure"""
    print("=" * 60)
    print("Validating Data Pipeline Project Structure")
    print("=" * 60)
    
    all_valid = True
    
    # Core directories
    print("\n[1] Core Directories")
    directories = [
        ('data_pipeline', 'Main package'),
        ('data_pipeline/ingestion', 'Ingestion module'),
        ('data_pipeline/transformation', 'Transformation module'),
        ('data_pipeline/masking', 'Masking module'),
        ('data_pipeline/utils', 'Utilities module'),
        ('airflow/dags', 'Airflow DAGs'),
        ('config', 'Configuration'),
        ('scripts', 'Scripts'),
        ('sql', 'SQL files'),
        ('docs', 'Documentation'),
        ('tests', 'Tests'),
    ]
    
    for path, desc in directories:
        if not check_directory_exists(path, desc):
            all_valid = False
    
    # Core files
    print("\n[2] Core Files")
    files = [
        ('README.md', 'Main README'),
        ('requirements.txt', 'Python dependencies'),
        ('setup.py', 'Setup configuration'),
        ('.gitignore', 'Git ignore file'),
        ('.env.example', 'Environment variables template'),
        ('config/pipeline_config.yaml', 'Pipeline configuration'),
    ]
    
    for path, desc in files:
        if not check_file_exists(path, desc):
            all_valid = False
    
    # Python modules
    print("\n[3] Python Modules")
    modules = [
        ('data_pipeline/__init__.py', 'Main package init'),
        ('data_pipeline/ingestion/base.py', 'Ingestion base'),
        ('data_pipeline/ingestion/database_source.py', 'Database source'),
        ('data_pipeline/ingestion/api_source.py', 'API source'),
        ('data_pipeline/ingestion/file_source.py', 'File source'),
        ('data_pipeline/transformation/spark_transformer.py', 'Spark transformer'),
        ('data_pipeline/transformation/star_schema.py', 'Star schema builder'),
        ('data_pipeline/masking/data_masker.py', 'Data masker'),
        ('data_pipeline/utils/s3_utils.py', 'S3 utilities'),
        ('data_pipeline/utils/config_loader.py', 'Config loader'),
    ]
    
    for path, desc in modules:
        if not check_file_exists(path, desc):
            all_valid = False
    
    # Airflow DAGs
    print("\n[4] Airflow Components")
    if not check_file_exists('airflow/dags/data_pipeline_dag.py', 'Main DAG'):
        all_valid = False
    
    # SQL files
    print("\n[5] SQL Files")
    sql_files = [
        ('sql/create_star_schema.sql', 'Star schema DDL'),
        ('sql/analytical_queries.sql', 'Sample queries'),
    ]
    
    for path, desc in sql_files:
        if not check_file_exists(path, desc):
            all_valid = False
    
    # Documentation
    print("\n[6] Documentation")
    docs = [
        ('docs/ARCHITECTURE.md', 'Architecture doc'),
        ('docs/POWERBI_INTEGRATION.md', 'Power BI guide'),
        ('docs/QUICKSTART.md', 'Quick start guide'),
    ]
    
    for path, desc in docs:
        if not check_file_exists(path, desc):
            all_valid = False
    
    # Scripts
    print("\n[7] Scripts")
    scripts = [
        ('scripts/setup_pipeline.py', 'Setup script'),
        ('scripts/generate_sample_data.py', 'Sample data generator'),
        ('scripts/run_pipeline.py', 'Pipeline runner'),
    ]
    
    for path, desc in scripts:
        if not check_file_exists(path, desc):
            all_valid = False
    
    # Summary
    print("\n" + "=" * 60)
    if all_valid:
        print("✓ All project structure checks passed!")
        print("=" * 60)
        print("\nNext steps:")
        print("1. Install dependencies: pip install -r requirements.txt")
        print("2. Configure environment: cp .env.example .env && nano .env")
        print("3. Generate sample data: python scripts/generate_sample_data.py")
        print("4. Run tests: python tests/test_pipeline.py")
        print("5. See docs/QUICKSTART.md for more information")
        return 0
    else:
        print("✗ Some checks failed!")
        print("=" * 60)
        return 1


if __name__ == '__main__':
    exit_code = validate_project_structure()
    sys.exit(exit_code)
