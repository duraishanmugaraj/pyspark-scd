from pyspark.sql.functions import col, rand, lit, hash, abs, expr
import random

def salt_table(df, key_column, num_partitions=10, salt_column="salt"):
    """
    Add a salt column to distribute data more evenly across partitions
    
    Parameters:
    -----------
    df: DataFrame - Data to salt
    key_column: str - Column to salt
    num_partitions: int - Number of partitions
    salt_column: str - Name for the salt column
    
    Returns:
    --------
    DataFrame - Data with salt column added
    """
    # Add a salt column with values 0 to num_partitions-1
    return df.withColumn(salt_column, abs(hash(col(key_column)) % lit(num_partitions)))

def demonstrate_salting():
    """Demonstrate data salting with examples"""
    from data_generator import get_spark_session, generate_customer_data
    from pyspark.sql.functions import count
    
    spark = get_spark_session("Salting_Demo")
    
    # Generate a larger dataset
    customers = generate_customer_data(spark, num_customers=1000)
    
    # Let's say we want to optimize queries that filter on segment
    # First, let's see the distribution of segments
    print("Distribution of customer segments:")
    customers.groupBy("segment").count().show()
    
    # Add a salt column
    salted_customers = salt_table(customers, "segment", num_partitions=5)
    
    print("\nData after salting:")
    salted_customers.select("customer_id", "name", "segment", "salt").show(10)
    
    # Check distribution across salts for each segment
    print("\nDistribution across salts for each segment:")
    salted_customers.groupBy("segment", "salt").count().orderBy("segment", "salt").show()
    
    # Explain how to use the salted data for queries
    print("\nExplanation of Salting:")
    print("1. Data Skew Problem:")
    print("   - When data is unevenly distributed, some partitions may have much more data than others")
    print("   - This leads to slower queries and 'hot spots' in distributed processing")
    print("\n2. Salting Solution:")
    print("   - We add a 'salt' column that spreads data evenly across partitions")
    print("   - The salt values are calculated from a hash of the key column")
    
    print("\n3. How to Query Salted Data:")
    print("   - For exact value queries, you must query all salt values:")
    print("   ```")
    print("   # Without salting:")
    print("   df.filter(col('segment') == 'Premium')")
    print("   ")
    print("   # With salting - you have to query all salts:")
    print("   from pyspark.sql.functions import lit")
    print("   results = spark.createDataFrame([], schema=salted_customers.schema)")
    print("   for salt_value in range(5):")
    print("       salt_query = salted_customers.filter((col('segment') == 'Premium') & (col('salt') == salt_value))")
    print("       results = results.union(salt_query)")
    print("   ```")
    
    # Show how to implement a salted join
    print("\n4. Implementing a Salted Join:")
    print("   ```")
    print("   # Without salting:")
    print("   df1.join(df2, on='customer_id')")
    print("   ")
    print("   # With salting:")
    print("   salted_df1 = salt_table(df1, 'customer_id', 10)")
    print("   salted_df2 = salt_table(df2, 'customer_id', 10)")
    print("   salted_join = salted_df1.join(salted_df2, on=['customer_id', 'salt'])")
    print("   ```")
    
    # Demonstrate salted join performance
    print("\n5. Performance Benefits:")
    print("   - More efficient resource utilization")
    print("   - Better parallelism and reduced skew")
    print("   - Faster query processing for skewed data")
    
    return salted_customers

if __name__ == "__main__":
    demonstrate_salting()