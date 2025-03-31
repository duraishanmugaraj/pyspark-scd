# PySpark Slowly Changing Dimensions (SCD) Implementation

This repository provides a comprehensive implementation of Slowly Changing Dimension (SCD) patterns in PySpark, along with a data salting technique to address data skew issues in distributed processing.

## Overview

Slowly Changing Dimensions (SCDs) are a fundamental concept in data warehousing that deals with how to handle changes to dimension attributes over time. This repository demonstrates implementation of the most common SCD types:

- **Type 1 SCD**: Overwrite old values with new values (no history)
- **Type 2 SCD**: Keep full history by adding new rows with effective dates
- **Type 3 SCD**: Keep limited history by adding columns for previous values

Additionally, this repository includes:

- **Data Salting**: A technique to distribute skewed data evenly across partitions
- **Data Generation Utilities**: Tools to create sample data for demonstrations

## Files in this Repository

- `scd_examples.ipynb`: Jupyter notebook with comprehensive examples of all SCD types
- `scd_type1.py`: Implementation of Type 1 SCD pattern
- `scd_type2.py`: Implementation of Type 2 SCD pattern
- `scd_type3.py`: Implementation of Type 3 SCD pattern
- `salting.py`: Implementation of data salting technique
- `data_generator.py`: Utilities to generate sample data

## Requirements

- Apache Spark 3.x
- Python 3.7+
- PySpark

## Usage

Each file contains both the implementation and a demonstration function. You can run any file directly to see a demonstration:

```bash
# Run Type 1 SCD demonstration
spark-submit scd_type1.py

# Run Type 2 SCD demonstration
spark-submit scd_type2.py

# Run Type 3 SCD demonstration
spark-submit scd_type3.py

# Run Data Salting demonstration
spark-submit salting.py
```

Or import the functions into your own code:

```python
from scd_type2 import apply_scd_type2
from data_generator import get_spark_session, generate_customer_data

# Create a Spark session
spark = get_spark_session("My_SCD_Application")

# Generate or load your dimension table
dimension_table = generate_customer_data(spark, num_customers=100)

# Apply Type 2 SCD logic
updated_dimension = apply_scd_type2(spark, dimension_table, updates_df)
```

## When to Use Each SCD Type

- **Type 1**: For attributes where history isn't important or when correcting errors
- **Type 2**: When full history needs to be preserved for analysis over time
- **Type 3**: When limited history (typically just the previous value) is sufficient

## Performance Considerations

- Type 2 SCD increases storage requirements significantly as history grows
- Consider partitioning Type 2 SCD tables by effective date for better query performance
- Use the data salting technique when dealing with skewed data distributions

## License

MIT