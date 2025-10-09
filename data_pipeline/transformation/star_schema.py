"""
Star Schema Builder
Creates fact and dimension tables for the data warehouse
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Dict, List, Optional
from .spark_transformer import SparkTransformer


class StarSchemaBuilder(SparkTransformer):
    """Build star schema with fact and dimension tables"""
    
    def __init__(self, spark_config: Dict):
        super().__init__(spark_config)
    
    def create_dimension_customer(self, customer_df: DataFrame) -> DataFrame:
        """Create customer dimension table"""
        return customer_df.select(
            F.monotonically_increasing_id().alias('customer_key'),
            F.col('customer_id'),
            F.col('first_name'),
            F.col('last_name'),
            F.col('email'),
            F.col('phone'),
            F.col('address'),
            F.col('city'),
            F.col('state'),
            F.col('zip_code'),
            F.col('country'),
            F.current_timestamp().alias('effective_date'),
            F.lit(None).cast('timestamp').alias('end_date'),
            F.lit(True).alias('is_current')
        ).dropDuplicates(['customer_id'])
    
    def create_dimension_product(self, product_df: DataFrame) -> DataFrame:
        """Create product dimension table"""
        return product_df.select(
            F.monotonically_increasing_id().alias('product_key'),
            F.col('product_id'),
            F.col('product_name'),
            F.col('category'),
            F.col('subcategory'),
            F.col('brand'),
            F.col('unit_price'),
            F.col('unit_cost'),
            F.current_timestamp().alias('effective_date'),
            F.lit(None).cast('timestamp').alias('end_date'),
            F.lit(True).alias('is_current')
        ).dropDuplicates(['product_id'])
    
    def create_dimension_date(self, start_date: str, end_date: str) -> DataFrame:
        """Create date dimension table"""
        date_df = self.spark.sql(f"""
            SELECT sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as date_array
        """)
        
        date_df = date_df.select(F.explode('date_array').alias('date'))
        
        return date_df.select(
            F.date_format('date', 'yyyyMMdd').cast('int').alias('date_key'),
            F.col('date').alias('full_date'),
            F.year('date').alias('year'),
            F.quarter('date').alias('quarter'),
            F.month('date').alias('month'),
            F.dayofmonth('date').alias('day'),
            F.dayofweek('date').alias('day_of_week'),
            F.weekofyear('date').alias('week_of_year'),
            F.date_format('date', 'MMMM').alias('month_name'),
            F.date_format('date', 'EEEE').alias('day_name')
        )
    
    def create_dimension_location(self, location_df: DataFrame) -> DataFrame:
        """Create location dimension table"""
        return location_df.select(
            F.monotonically_increasing_id().alias('location_key'),
            F.col('location_id'),
            F.col('store_name'),
            F.col('address'),
            F.col('city'),
            F.col('state'),
            F.col('zip_code'),
            F.col('country'),
            F.col('region')
        ).dropDuplicates(['location_id'])
    
    def create_fact_sales(self, sales_df: DataFrame, 
                          dim_customer: DataFrame,
                          dim_product: DataFrame,
                          dim_date: DataFrame,
                          dim_location: Optional[DataFrame] = None) -> DataFrame:
        """Create sales fact table"""
        
        # Join with dimensions to get surrogate keys
        fact = sales_df \
            .join(dim_customer, sales_df.customer_id == dim_customer.customer_id, 'left') \
            .join(dim_product, sales_df.product_id == dim_product.product_id, 'left') \
            .join(dim_date, 
                  F.date_format(sales_df.transaction_date, 'yyyyMMdd').cast('int') == dim_date.date_key,
                  'left')
        
        if dim_location is not None:
            fact = fact.join(dim_location, 
                           sales_df.location_id == dim_location.location_id, 
                           'left')
        
        # Select fact columns
        fact_columns = [
            F.monotonically_increasing_id().alias('sales_key'),
            dim_customer.customer_key,
            dim_product.product_key,
            dim_date.date_key,
        ]
        
        if dim_location is not None:
            fact_columns.append(dim_location.location_key)
        
        fact_columns.extend([
            F.col('transaction_id'),
            F.col('quantity'),
            F.col('unit_price'),
            F.col('discount'),
            (F.col('quantity') * F.col('unit_price') * (1 - F.col('discount'))).alias('total_amount'),
            F.col('tax_amount'),
            F.col('transaction_date'),
            F.col('transaction_timestamp')
        ])
        
        return fact.select(*fact_columns)
    
    def build_star_schema(self, 
                         sales_df: DataFrame,
                         customer_df: DataFrame,
                         product_df: DataFrame,
                         location_df: Optional[DataFrame] = None,
                         start_date: str = '2020-01-01',
                         end_date: str = '2025-12-31') -> Dict[str, DataFrame]:
        """Build complete star schema"""
        
        dim_customer = self.create_dimension_customer(customer_df)
        dim_product = self.create_dimension_product(product_df)
        dim_date = self.create_dimension_date(start_date, end_date)
        
        dimensions = {
            'dim_customer': dim_customer,
            'dim_product': dim_product,
            'dim_date': dim_date
        }
        
        if location_df is not None:
            dim_location = self.create_dimension_location(location_df)
            dimensions['dim_location'] = dim_location
            fact_sales = self.create_fact_sales(sales_df, dim_customer, dim_product, 
                                               dim_date, dim_location)
        else:
            fact_sales = self.create_fact_sales(sales_df, dim_customer, dim_product, 
                                               dim_date)
        
        dimensions['fact_sales'] = fact_sales
        
        return dimensions
