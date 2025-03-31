from pyspark.sql.functions import col, when, lit

def apply_scd_type3(spark, dim_table, updates, tracked_column="segment"):
    """
    Apply Type 3 SCD - Keep limited history with previous value columns
    
    Parameters:
    -----------
    spark: SparkSession
    dim_table: DataFrame - Current dimension table
    updates: DataFrame - New data (updates)
    tracked_column: str - The column for which we want to track previous values
    
    Returns:
    --------
    DataFrame - Updated dimension table with previous value columns
    """
    # Ensure dimension table has previous value columns
    if f"previous_{tracked_column}" not in dim_table.columns:
        dim_table = dim_table.withColumn(f"previous_{tracked_column}", lit(None).cast("string"))
    
    if "change_date" not in dim_table.columns:
        dim_table = dim_table.withColumn("change_date", lit(None).cast("date"))
    
    # Find records that need to be updated
    join_condition = (dim_table["customer_id"] == updates["customer_id"]) & \
                    (dim_table[tracked_column] != updates[tracked_column])
    
    # Join dimension table with updates
    joined_df = dim_table.join(updates, on="customer_id", how="left_outer")
    
    # Create updated dimension table
    columns_to_select = []
    for column in dim_table.columns:
        if column == "customer_id":
            columns_to_select.append(col(f"dim_table.{column}"))
        elif column == tracked_column:
            # If there's an update for this column, use the new value
            columns_to_select.append(
                when(col(f"updates.{column}").isNotNull() & (col(f"dim_table.{column}") != col(f"updates.{column}")), 
                     col(f"updates.{column}"))
                .otherwise(col(f"dim_table.{column}"))
                .alias(column)
            )
        elif column == f"previous_{tracked_column}":
            # If there's an update for the tracked column, use the current value as previous
            columns_to_select.append(
                when(col(f"updates.{tracked_column}").isNotNull() & (col(f"dim_table.{tracked_column}") != col(f"updates.{tracked_column}")), 
                     col(f"dim_table.{tracked_column}"))
                .otherwise(col(f"dim_table.{column}"))
                .alias(column)
            )
        elif column == "change_date":
            # If there's an update for the tracked column, set change date
            columns_to_select.append(
                when(col(f"updates.{tracked_column}").isNotNull() & (col(f"dim_table.{tracked_column}") != col(f"updates.{tracked_column}")), 
                     col("updates.effective_date"))
                .otherwise(col(f"dim_table.{column}"))
                .alias(column)
            )
        else:
            # For other columns, use updated value if available
            columns_to_select.append(
                when(col(f"updates.{column}").isNotNull(), col(f"updates.{column}"))
                .otherwise(col(f"dim_table.{column}"))
                .alias(column)
            )
    
    updated_df = joined_df.select(*columns_to_select)
    
    # Add any new records
    dim_ids = dim_table.select("customer_id").distinct()
    new_records = updates.join(dim_ids, on="customer_id", how="left_anti")
    
    # Add the previous value columns to new records
    if f"previous_{tracked_column}" not in new_records.columns:
        new_records = new_records.withColumn(f"previous_{tracked_column}", lit(None).cast("string"))
    
    if "change_date" not in new_records.columns:
        new_records = new_records.withColumn("change_date", lit(None).cast("date"))
    
    # Union updated records with new records
    result = updated_df.union(new_records.select(*updated_df.columns))
    
    print(f"Type 3 SCD Applied: {result.count()} total records")
    return result

def demonstrate_scd_type3():
    """Demonstrate Type 3 SCD with example data"""
    from data_generator import get_spark_session, generate_customer_data, generate_customer_updates
    
    spark = get_spark_session("SCD_Type3_Demo")
    
    # Generate initial customer dimension
    customers = generate_customer_data(spark, num_customers=10)
    
    # Add the previous value columns
    customers = customers.withColumn("previous_segment", lit(None).cast("string"))
    customers = customers.withColumn("change_date", lit(None).cast("date"))
    
    print("Original Customer Dimension:")
    customers.select("customer_id", "name", "segment", "previous_segment", "change_date").show(5)
    
    # Generate updates
    updates = generate_customer_updates(spark, num_updates=5)
    print("\nUpdates to be applied:")
    updates.select("customer_id", "name", "segment", "effective_date").show(5)
    
    # Apply Type 3 SCD
    updated_customers = apply_scd_type3(spark, customers, updates, tracked_column="segment")
    print("\nCustomer Dimension after Type 3 SCD:")
    updated_customers.select("customer_id", "name", "segment", "previous_segment", "change_date").orderBy("customer_id").show(10)
    
    # Apply a second update to demonstrate tracking changes
    second_updates = generate_customer_updates(spark, num_updates=3)
    print("\nSecond round of updates:")
    second_updates.select("customer_id", "name", "segment", "effective_date").show(3)
    
    # Apply Type 3 SCD again
    final_customers = apply_scd_type3(spark, updated_customers, second_updates, tracked_column="segment")
    print("\nCustomer Dimension after second Type 3 SCD update:")
    final_customers.select("customer_id", "name", "segment", "previous_segment", "change_date").orderBy("customer_id").show(10)
    
    # Explain the result
    print("\nExplanation:")
    print("In Type 3 SCD, we've stored limited history by:")
    print("1. Adding a 'previous_segment' column to store the old value")
    print("2. Adding a 'change_date' column to record when the change happened")
    print("3. Keeping only the most recent previous value (not the entire history)")
    print("This is useful when you need quick access to current and previous values, but don't need the entire history.")
    
    return final_customers

if __name__ == "__main__":
    demonstrate_scd_type3()