from pyspark.sql.functions import col, lit, when

def apply_scd_type1(spark, dim_table, updates):
    """
    Apply Type 1 SCD - Overwrite the existing values with new values
    
    Parameters:
    -----------
    spark: SparkSession
    dim_table: DataFrame - Current dimension table
    updates: DataFrame - New data (updates)
    
    Returns:
    --------
    DataFrame - Updated dimension table
    """
    # Make sure both tables have the same schema
    updates = updates.select(dim_table.columns)
    
    # First, we find all records that need to be updated
    joined_df = dim_table.alias("dim").join(
        updates.alias("updates"),
        on=["customer_id"],
        how="left_outer"
    )
    
    # Create a new DataFrame with updated values
    # If there's a corresponding record in updates, use that value; otherwise, keep the original
    columns_to_select = []
    for column in dim_table.columns:
        if column == "customer_id":
            columns_to_select.append(col("dim.customer_id"))
        else:
            columns_to_select.append(
                when(col(f"updates.{column}").isNotNull(), col(f"updates.{column}"))
                .otherwise(col(f"dim.{column}"))
                .alias(column)
            )
    
    updated_df = joined_df.select(*columns_to_select)
    
    # Add any new records from updates that aren't in dimension table
    dim_ids = dim_table.select("customer_id").distinct()
    new_records = updates.join(dim_ids, on=["customer_id"], how="left_anti")
    
    # Combine updated records with new records
    result = updated_df.union(new_records)
    
    print(f"Type 1 SCD Applied: {result.count()} total records ({new_records.count()} new records)")
    
    return result

def demonstrate_scd_type1():
    """Demonstrate Type 1 SCD with example data"""
    from data_generator import get_spark_session, generate_customer_data, generate_customer_updates
    
    spark = get_spark_session("SCD_Type1_Demo")
    
    # Generate initial customer dimension and display
    customers = generate_customer_data(spark, num_customers=10)
    print("Original Customer Dimension:")
    customers.show(5)
    
    # Generate updates
    updates = generate_customer_updates(spark, num_updates=5)
    print("\nUpdates to be applied:")
    updates.show(5)
    
    # Apply Type 1 SCD
    updated_customers = apply_scd_type1(spark, customers, updates)
    print("\nCustomer Dimension after Type 1 SCD:")
    updated_customers.show(10)
    
    # Explain the result
    print("\nExplanation:")
    print("In Type 1 SCD, we've simply overwritten the old values with new values.")
    print("No history is maintained, so we can't tell what the previous values were.")
    print("This approach is good for correcting errors or updating non-historical attributes.")
    
    return updated_customers

if __name__ == "__main__":
    demonstrate_scd_type1()