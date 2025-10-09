# Makefile for Data Engineering Pipeline

.PHONY: help install setup validate generate-data test run clean

help:
	@echo "Data Engineering Pipeline - Available Commands"
	@echo "==============================================="
	@echo "make install          - Install Python dependencies"
	@echo "make setup            - Setup environment and configuration"
	@echo "make validate         - Validate project structure"
	@echo "make generate-data    - Generate sample data for testing"
	@echo "make test             - Run unit tests"
	@echo "make run              - Run the pipeline locally"
	@echo "make clean            - Clean temporary files and caches"
	@echo "make airflow-init     - Initialize Airflow database"
	@echo "make airflow-start    - Start Airflow webserver"

install:
	@echo "Installing dependencies..."
	pip install -r requirements.txt

setup:
	@echo "Setting up environment..."
	cp -n .env.example .env || echo ".env already exists"
	@echo "Please edit .env with your configuration"

validate:
	@echo "Validating project structure..."
	python scripts/validate_structure.py

generate-data:
	@echo "Generating sample data..."
	python scripts/generate_sample_data.py

test:
	@echo "Running tests..."
	python tests/test_pipeline.py

run:
	@echo "Running pipeline..."
	python scripts/run_pipeline.py

clean:
	@echo "Cleaning temporary files..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache
	rm -rf build dist
	@echo "Clean complete!"

airflow-init:
	@echo "Initializing Airflow..."
	export AIRFLOW_HOME=$$(pwd)/airflow && \
	airflow db init && \
	airflow users create \
		--username admin \
		--firstname Admin \
		--lastname User \
		--role Admin \
		--email admin@example.com \
		--password admin

airflow-start:
	@echo "Starting Airflow webserver..."
	@echo "Access UI at http://localhost:8080"
	@echo "Username: admin, Password: admin"
	export AIRFLOW_HOME=$$(pwd)/airflow && \
	airflow webserver --port 8080
