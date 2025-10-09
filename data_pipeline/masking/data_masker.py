"""
Data Masking Module
Implements various data masking techniques for sensitive data
"""

import hashlib
import re
from typing import Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import secrets


class DataMasker:
    """Data masking utilities for protecting sensitive information"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.masking_enabled = config.get('enabled', True)
        self.fields_config = config.get('fields', {})
    
    @staticmethod
    def hash_value(value: str, algorithm: str = 'sha256') -> str:
        """Hash a value using specified algorithm"""
        if value is None:
            return None
        
        if algorithm == 'sha256':
            return hashlib.sha256(value.encode()).hexdigest()
        elif algorithm == 'md5':
            return hashlib.md5(value.encode()).hexdigest()
        else:
            return hashlib.sha256(value.encode()).hexdigest()
    
    @staticmethod
    def mask_email(email: str) -> str:
        """Mask email address"""
        if email is None or '@' not in email:
            return email
        
        username, domain = email.split('@')
        if len(username) <= 2:
            masked_username = username[0] + '*' * (len(username) - 1)
        else:
            masked_username = username[0] + '*' * (len(username) - 2) + username[-1]
        
        return f"{masked_username}@{domain}"
    
    @staticmethod
    def mask_phone(phone: str, pattern: str = "XXX-XXX-####") -> str:
        """Mask phone number according to pattern"""
        if phone is None:
            return None
        
        # Remove non-digit characters
        digits = re.sub(r'\D', '', phone)
        
        if len(digits) < 4:
            return 'XXX-XXX-XXXX'
        
        # Show only last 4 digits
        last_four = digits[-4:]
        return pattern.replace('####', last_four)
    
    @staticmethod
    def mask_ssn(ssn: str) -> str:
        """Mask SSN showing only last 4 digits"""
        if ssn is None:
            return None
        
        digits = re.sub(r'\D', '', ssn)
        
        if len(digits) < 4:
            return 'XXX-XX-XXXX'
        
        last_four = digits[-4:]
        return f"XXX-XX-{last_four}"
    
    @staticmethod
    def mask_credit_card(card_number: str) -> str:
        """Mask credit card showing only last 4 digits"""
        if card_number is None:
            return None
        
        digits = re.sub(r'\D', '', card_number)
        
        if len(digits) < 4:
            return 'XXXX-XXXX-XXXX-XXXX'
        
        last_four = digits[-4:]
        return f"XXXX-XXXX-XXXX-{last_four}"
    
    @staticmethod
    def tokenize(value: str) -> str:
        """Generate a random token for the value"""
        if value is None:
            return None
        
        # Generate a consistent token based on value
        hash_obj = hashlib.sha256(value.encode())
        token = hash_obj.hexdigest()[:16].upper()
        return f"TOK_{token}"
    
    def apply_masking_spark(self, df: DataFrame, field_name: str, 
                           method: str, **kwargs) -> DataFrame:
        """Apply masking to a Spark DataFrame column"""
        
        if not self.masking_enabled:
            return df
        
        if field_name not in df.columns:
            return df
        
        if method == 'hash':
            algorithm = kwargs.get('algorithm', 'sha256')
            hash_udf = F.udf(lambda x: self.hash_value(x, algorithm), StringType())
            return df.withColumn(field_name, hash_udf(F.col(field_name)))
        
        elif method == 'mask':
            if 'email' in field_name.lower():
                mask_udf = F.udf(self.mask_email, StringType())
            elif 'phone' in field_name.lower():
                pattern = kwargs.get('pattern', "XXX-XXX-####")
                mask_udf = F.udf(lambda x: self.mask_phone(x, pattern), StringType())
            elif 'ssn' in field_name.lower():
                mask_udf = F.udf(self.mask_ssn, StringType())
            elif 'credit_card' in field_name.lower() or 'card' in field_name.lower():
                mask_udf = F.udf(self.mask_credit_card, StringType())
            else:
                # Generic masking
                mask_udf = F.udf(lambda x: 'XXXXX' if x else None, StringType())
            
            return df.withColumn(field_name, mask_udf(F.col(field_name)))
        
        elif method == 'tokenize':
            tokenize_udf = F.udf(self.tokenize, StringType())
            return df.withColumn(field_name, tokenize_udf(F.col(field_name)))
        
        return df
    
    def apply_masking_policy(self, df: DataFrame) -> DataFrame:
        """Apply masking policy based on configuration"""
        
        if not self.masking_enabled:
            return df
        
        masked_df = df
        
        for field_name, field_config in self.fields_config.items():
            if field_name in df.columns:
                method = field_config.get('method', 'mask')
                
                if method == 'hash':
                    algorithm = field_config.get('algorithm', 'sha256')
                    masked_df = self.apply_masking_spark(masked_df, field_name, 
                                                        method, algorithm=algorithm)
                elif method == 'mask':
                    pattern = field_config.get('pattern')
                    if pattern:
                        masked_df = self.apply_masking_spark(masked_df, field_name, 
                                                           method, pattern=pattern)
                    else:
                        masked_df = self.apply_masking_spark(masked_df, field_name, method)
                elif method == 'tokenize':
                    masked_df = self.apply_masking_spark(masked_df, field_name, method)
        
        return masked_df
