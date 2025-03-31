from pyspark.sql.functions import col, lit, when, current_date, datediff, row_number, lead
from pyspark.sql.window import Window
import datetime

def apply_scd_type2(spark, dim_table, updates):
    """
    Apply Type 2 SCD - Keep full history by creating new rows for changes
    
    Parameters:
    -----------
    spark: SparkSession
    dim_table: DataFrame - Current dimension table
    updates: DataFrame - New data (updates)
    
    Returns:
    --------
    DataFrame - Updated dimension table with historical records
    """
    # 1. Mark current records in the dimension table that will be updated as not current
    # Find the keys that have updates
    keys_to_update = updates.select("customer_id").distinct()
    
    # Get the records from dimension table that will be affected
    records_to_update = dim_table.join(keys_to_update, on=["customer_id"], how="inner")
    records_to_update = records_to_update.filter(col("is_current") == True)
    
    # Get the records from dimension table that will remain unchanged
    unchanged_records = dim_table.join(keys_to_update, on=["customer_id"], how="left_anti")
    
    # Create a copy of the records to update with is_current = False
    records_to_expire = records_to_update.withColumn("is_current", lit(False))
    records_to_expire = records_to_expire.withColumn(
        "end_date", current_date()
    )
    
    # 2. Add new records from the updates with is_current = True
    new_records = updates.withColumn("is_current", lit(True))
    new_records = new_records.withColumn("end_date", lit(None).cast("date"))
    
    # 3. Combine all records together
    result = unchanged_records.union(records_to_expire).union(new_records)
    
    # 4. Fill end_date for records that were already not current
    result = result.withColumn(
        "end_date", 
        when(col("is_current") == False & col("end_date").isNull(), current_date())
        .otherwise(col("end_date"))
    )
    
    print(f"Type 2 SCD Applied: {result.count()} total records")
    print(f"  - {unchanged_records.count()} unchanged records")
    print(f"  - {records_to_expire.count()} expired records")
    print(f"  - {new_records.count()} new records")
    
    return result

def demonstrate_scd_type2():
    """Demonstrate Type 2 SCD with example data"""
    from data_generator import get_spark_session, generate_customer_data, generate_customer_updates
    from pyspark.sql.functions import col
    
    spark = get_spark_session("SCD_Type2_Demo")
    
    # Generate initial customer dimension with effective_date and is_current
    customers_raw = generate_customer_data(spark, num_customers=10)
    
    # Add end_date column (null for current records)
    customers = customers_raw.withColumn("end_date", lit(None).cast("date"))
    
    print("Original Customer Dimension:")
    customers.orderBy("customer_id").show(5)
    
    # Generate updates
    updates = generate_customer_updates(spark, num_updates=5)
    print("\nUpdates to be applied:")
    updates.show(5)
    
    # Apply Type 2 SCD
    updated_customers = apply_scd_type2(spark, customers, updates)
    print("\nCustomer Dimension after Type 2 SCD:")
    updated_customers.orderBy("customer_id", col("effective_date").desc()).show(15)
    
    # Explain the result
    print("\nExplanation:")
    print("In Type 2 SCD, we've preserved the full history by:")
    print("1. Marking the previous rows as not current (is_current = False)")
    print("2. Setting an end_date for those rows")
    print("3. Adding new rows with the updated values and is_current = True")
    print("This allows us to see both current and historical values for each customer.")
    
    return updated_customers

if __name__ == "__main__":
    demonstrate_scd_type2()