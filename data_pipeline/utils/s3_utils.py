"""
S3 Utilities
Helper functions for interacting with AWS S3
"""

import boto3
import logging
from typing import List, Dict, Any, Optional
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3Manager:
    """Manages S3 operations for the data pipeline"""
    
    def __init__(self, region_name: str, bucket_name: str):
        self.region_name = region_name
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.s3_resource = boto3.resource('s3', region_name=region_name)
        self.logger = logger
    
    def create_bucket(self):
        """Create S3 bucket if it doesn't exist"""
        try:
            if self.region_name == 'us-east-1':
                self.s3_client.create_bucket(Bucket=self.bucket_name)
            else:
                self.s3_client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': self.region_name}
                )
            self.logger.info(f"Bucket {self.bucket_name} created successfully")
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                self.logger.info(f"Bucket {self.bucket_name} already exists")
            else:
                raise
    
    def list_objects(self, prefix: str = '') -> List[Dict[str, Any]]:
        """List objects in bucket with given prefix"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' in response:
                return response['Contents']
            return []
        
        except ClientError as e:
            self.logger.error(f"Error listing objects: {str(e)}")
            return []
    
    def upload_file(self, local_path: str, s3_key: str):
        """Upload file to S3"""
        try:
            self.s3_client.upload_file(local_path, self.bucket_name, s3_key)
            self.logger.info(f"Uploaded {local_path} to s3://{self.bucket_name}/{s3_key}")
        except ClientError as e:
            self.logger.error(f"Error uploading file: {str(e)}")
            raise
    
    def download_file(self, s3_key: str, local_path: str):
        """Download file from S3"""
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            self.logger.info(f"Downloaded s3://{self.bucket_name}/{s3_key} to {local_path}")
        except ClientError as e:
            self.logger.error(f"Error downloading file: {str(e)}")
            raise
    
    def delete_object(self, s3_key: str):
        """Delete object from S3"""
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            self.logger.info(f"Deleted s3://{self.bucket_name}/{s3_key}")
        except ClientError as e:
            self.logger.error(f"Error deleting object: {str(e)}")
            raise
    
    def copy_object(self, source_key: str, dest_key: str):
        """Copy object within S3"""
        try:
            copy_source = {'Bucket': self.bucket_name, 'Key': source_key}
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=self.bucket_name,
                Key=dest_key
            )
            self.logger.info(f"Copied {source_key} to {dest_key}")
        except ClientError as e:
            self.logger.error(f"Error copying object: {str(e)}")
            raise
    
    def get_s3_path(self, prefix: str, filename: str = '') -> str:
        """Get full S3 path"""
        if filename:
            return f"s3://{self.bucket_name}/{prefix}{filename}"
        return f"s3://{self.bucket_name}/{prefix}"
    
    def setup_data_lake_structure(self, prefixes: List[str]):
        """Setup data lake folder structure"""
        for prefix in prefixes:
            # Create a dummy file to establish the prefix
            dummy_key = f"{prefix}.keep"
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=dummy_key,
                Body=b''
            )
        self.logger.info("Data lake structure created")
