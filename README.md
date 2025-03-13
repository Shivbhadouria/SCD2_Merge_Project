# SCD Type 2 Implementation with PySpark and Delta Lake

## Project Overview
This mini project demonstrates the implementation of Slowly Changing Dimensions Type 2 (SCD2) using **PySpark, Delta Lake, and PyDeequ** for data quality checks. The project involves processing incremental booking and customer data, applying data quality checks, performing business transformations, and maintaining an SCD Type 2 dimension table for customer records.

## Technologies Used
- **Apache Spark (PySpark)** for data processing
- **Delta Lake** for ACID-compliant data storage
- **Databricks** as the execution environment
- **PyDeequ** for data quality checks
- **SQL** for data transformations

## Project Workflow
1. **Parameter Handling:** The arrival date (`arrival_date`) is taken as an input parameter to load the correct data files.
2. **Data Ingestion:**
   - Booking and customer data are read from CSV files.
   - Data is loaded into PySpark DataFrames.
3. **Data Quality Checks:**
   - Booking data checks include uniqueness, completeness, and non-negativity constraints.
   - Customer data checks include uniqueness and completeness constraints.
4. **Transformations:**
   - Booking data is enriched with an ingestion timestamp.
   - Booking data is joined with customer data.
   - Total cost after discount is calculated.
   - Aggregations are performed based on booking type and customer ID.
5. **Fact Table Handling:**
   - If the fact table exists, new data is merged.
   - Otherwise, the transformed data is written as a new Delta table.
6. **SCD Type 2 Implementation:**
   - If the customer dimension table exists, the new data is merged based on `customer_id`.
   - The `valid_to` column is updated for previous records.
   - New records are appended with `valid_from` and `valid_to` fields.

## File Structure
```
scd2-mini-project/
│── data/
│   ├── booking_data/ (Sample booking CSV files)
│   ├── customer_data/ (Sample customer CSV files)
│── scripts/
│   ├── scd2_etl.py  # Main ETL script for SCD2 processing
│── README.md  # Project documentation
```


## Future Enhancements
- Implement automated scheduling for daily incremental loads.
- Integrate real-time streaming using Spark Structured Streaming.
- Add versioning and rollback mechanisms for the SCD2 table.

## Conclusion
This project showcases an end-to-end data pipeline for incremental booking data processing and SCD Type 2 management using PySpark and Delta Lake. The integration of PyDeequ ensures data quality, while Delta Lake enables efficient storage and querying.

---
Feel free to contribute, raise issues, or suggest improvements!


