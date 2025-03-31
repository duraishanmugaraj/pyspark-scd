from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
from datetime import datetime, timedelta
import random

def get_spark_session(app_name="SCD_Implementation"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def generate_customer_data(spark, num_customers=100):
    """Generate initial customer data"""
    
    data = []
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego"]
    segments = ["Premium", "Standard", "Basic"]
    
    for i in range(1, num_customers + 1):
        data.append({
            "customer_id": f"C{i:03d}",
            "name": f"Customer {i}",
            "address": f"{random.randint(1, 9999)} Main St",
            "city": random.choice(cities),
            "segment": random.choice(segments),
            "credit_score": random.randint(580, 850),
            "effective_date": datetime.now().date() - timedelta(days=random.randint(30, 365)),
            "is_current": True
        })
    
    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("credit_score", IntegerType(), True),
        StructField("effective_date", DateType(), True),
        StructField("is_current", BooleanType(), True)
    ])
    
    return spark.createDataFrame(data, schema)

def generate_customer_updates(spark, num_updates=30):
    """Generate random updates to customer data"""
    
    data = []
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "Miami"]
    segments = ["Premium", "Standard", "Basic", "Elite"]
    
    for i in range(1, num_updates + 1):
        customer_id = f"C{random.randint(1, 100):03d}"
        update_type = random.choice(["name", "address", "city", "segment", "credit_score"])
        
        if update_type == "name":
            data.append({
                "customer_id": customer_id,
                "name": f"Updated Customer {i}",
                "address": f"{random.randint(1, 9999)} Main St",
                "city": random.choice(cities),
                "segment": random.choice(segments),
                "credit_score": random.randint(580, 850),
                "effective_date": datetime.now().date(),
                "is_current": True
            })
        elif update_type in ["address", "city"]:
            data.append({
                "customer_id": customer_id,
                "name": f"Customer {i}",
                "address": f"{random.randint(1, 9999)} New Ave",
                "city": random.choice(cities),
                "segment": random.choice(segments),
                "credit_score": random.randint(580, 850),
                "effective_date": datetime.now().date(),
                "is_current": True
            })
        elif update_type == "segment":
            data.append({
                "customer_id": customer_id,
                "name": f"Customer {i}",
                "address": f"{random.randint(1, 9999)} Main St",
                "city": random.choice(cities),
                "segment": random.choice(segments),
                "credit_score": random.randint(580, 850),
                "effective_date": datetime.now().date(),
                "is_current": True
            })
        elif update_type == "credit_score":
            data.append({
                "customer_id": customer_id,
                "name": f"Customer {i}",
                "address": f"{random.randint(1, 9999)} Main St",
                "city": random.choice(cities),
                "segment": random.choice(segments),
                "credit_score": random.randint(580, 850),
                "effective_date": datetime.now().date(),
                "is_current": True
            })
    
    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("credit_score", IntegerType(), True),
        StructField("effective_date", DateType(), True),
        StructField("is_current", BooleanType(), True)
    ])
    
    return spark.createDataFrame(data, schema)