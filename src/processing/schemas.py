import os
import requests
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

def get_binance_schema():
    """
    Defines the exact structure of the incoming JSON payload from Binance.
    Used by Spark as the PySpark StructType for DataFrame operations.
    """
    return StructType([
        StructField("e", StringType(), True),   # Event type
        StructField("E", LongType(), True),     # Event time (Unix epoch in ms)
        StructField("s", StringType(), True),   # Symbol
        StructField("p", StringType(), True),   # Price (comes as string from Binance)
        StructField("q", StringType(), True),   # Quantity (comes as string)
        StructField("T", LongType(), True)      # Trade time
    ])

def get_schema_registry_config():
    """
    Returns the Schema Registry URL from environment variables.
    """
    return os.environ.get('SCHEMA_REGISTRY_URL', 'http://localhost:8081')

def fetch_avro_schema_from_registry(topic: str) -> str:
    """
    Fetches the latest Avro schema string from Schema Registry for the given topic.
    The subject name follows the default TopicNameStrategy: '<topic>-value'.
    """
    sr_url = get_schema_registry_config()
    subject = f"{topic}-value"
    url = f"{sr_url}/subjects/{subject}/versions/latest"
    
    response = requests.get(url)
    response.raise_for_status()
    return response.json()["schema"]