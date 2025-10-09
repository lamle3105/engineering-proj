"""
Configuration Loader
Loads and manages pipeline configuration
"""

import yaml
import os
from typing import Dict, Any
from dotenv import load_dotenv


class ConfigLoader:
    """Loads configuration from YAML and environment variables"""
    
    def __init__(self, config_path: str = None):
        # Load environment variables
        load_dotenv()
        
        # Load YAML configuration
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(__file__), 
                '../../config/pipeline_config.yaml'
            )
        
        self.config = self._load_yaml_config(config_path)
        self._substitute_env_vars()
    
    def _load_yaml_config(self, config_path: str) -> Dict[str, Any]:
        """Load YAML configuration file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _substitute_env_vars(self):
        """Substitute environment variables in configuration"""
        self._recursive_substitute(self.config)
    
    def _recursive_substitute(self, obj):
        """Recursively substitute environment variables"""
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
                    env_var = value[2:-1]
                    obj[key] = os.getenv(env_var, value)
                elif isinstance(value, (dict, list)):
                    self._recursive_substitute(value)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                if isinstance(item, str) and item.startswith('${') and item.endswith('}'):
                    env_var = item[2:-1]
                    obj[i] = os.getenv(env_var, item)
                elif isinstance(item, (dict, list)):
                    self._recursive_substitute(item)
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value by dot-separated key path
        Example: config.get('spark.app_name')
        """
        keys = key_path.split('.')
        value = self.config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value
    
    def get_aws_config(self) -> Dict[str, str]:
        """Get AWS configuration"""
        return {
            'region': os.getenv('AWS_REGION', 'us-east-1'),
            'access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
            'secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
            'bucket_name': os.getenv('S3_BUCKET_NAME')
        }
    
    def get_db_config(self) -> Dict[str, str]:
        """Get database configuration"""
        return {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }
    
    def get_spark_config(self) -> Dict[str, Any]:
        """Get Spark configuration"""
        return self.get('spark', {})
    
    def get_data_sources(self) -> list:
        """Get data sources configuration"""
        return self.get('data_sources', [])
    
    def get_masking_config(self) -> Dict[str, Any]:
        """Get data masking configuration"""
        return self.get('data_masking', {})
