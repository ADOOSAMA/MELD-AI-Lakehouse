# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer ETL - Fixed Version
# MAGIC 
# MAGIC Create Silver layer data from Bronze layer data, fix column count mismatch issues

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Bronze Layer Data

# COMMAND ----------

# Read Bronze layer data
chart_bronze = spark.read.csv("/FileStore/icu-data/bronze/bronze_chartevent", header=True, inferSchema=True)
lab_bronze = spark.read.csv("/FileStore/icu-data/bronze/bronze_labevent", header=True, inferSchema=True)
mapping_bronze = spark.read.csv("/FileStore/icu-data/bronze/bronze_itemid_mapping", header=True, inferSchema=True)

print("Bronze layer data reading completed")
print("Monitoring data rows:", chart_bronze.count())
print("Lab data rows:", lab_bronze.count())
print("Mapping data rows:", mapping_bronze.count())

# Check column count
print("\nColumn count check:")
print("Monitoring data columns:", len(chart_bronze.columns))
print("Lab data columns:", len(lab_bronze.columns))
print("Mapping data columns:", len(mapping_bronze.columns))

print("\nMonitoring data column names:", chart_bronze.columns)
print("Lab data column names:", lab_bronze.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Aggregation and Transformation - Fixed Version

# COMMAND ----------

from pyspark.sql.functions import *

# Monitoring data aggregation - by patient and time window
chart_silver = chart_bronze \
    .withColumn("charttime_parsed", to_timestamp(col("charttime_parsed"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("date", to_date(col("charttime_parsed"))) \
    .withColumn("hour", hour(col("charttime_parsed"))) \
    .groupBy("patient_id", "itemid", "item_name", "date", "hour") \
    .agg(
        avg("valuenum").alias("avg_value"),
        max("valuenum").alias("max_value"),
        min("valuenum").alias("min_value"),
        count("valuenum").alias("measurement_count"),
        avg("data_quality_score").alias("avg_quality_score")
    ) \
    .withColumn("data_type", lit("chart")) \
    .withColumn("etl_timestamp", current_timestamp())

print("Monitoring data aggregation completed")
print("Aggregated data rows:", chart_silver.count())
print("Monitoring data columns:", len(chart_silver.columns))
print("Monitoring data column names:", chart_silver.columns)

# Lab data aggregation - by patient and date, add hour column to match monitoring data
lab_silver = lab_bronze \
    .withColumn("charttime_parsed", to_timestamp(col("charttime_parsed"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("date", to_date(col("charttime_parsed"))) \
    .withColumn("hour", hour(col("charttime_parsed"))) \
    .groupBy("patient_id", "itemid", "item_name", "date", "hour") \
    .agg(
        avg("valuenum").alias("avg_value"),
        max("valuenum").alias("max_value"),
        min("valuenum").alias("min_value"),
        count("valuenum").alias("measurement_count"),
        avg("data_quality_score").alias("avg_quality_score")
    ) \
    .withColumn("data_type", lit("lab")) \
    .withColumn("etl_timestamp", current_timestamp())

print("Lab data aggregation completed")
print("Aggregated data rows:", lab_silver.count())
print("Lab data columns:", len(lab_silver.columns))
print("Lab data column names:", lab_silver.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Merging and Standardization - Fixed Version

# COMMAND ----------

# Verify column count match
print("Pre-merge column count check:")
print("Monitoring data columns:", len(chart_silver.columns))
print("Lab data columns:", len(lab_silver.columns))

if len(chart_silver.columns) == len(lab_silver.columns):
    print(" Column count matches, can merge")
else:
    print(" Column count mismatch, needs fixing")

# Merge monitoring and lab data
combined_silver = chart_silver.union(lab_silver)

print("Data merge completed")
print("Merged data rows:", combined_silver.count())
print("Merged columns:", len(combined_silver.columns))

# Add risk scoring
combined_silver = combined_silver \
    .withColumn("risk_level", 
                when(col("avg_value") < 0.5, "Critical")
                .when(col("avg_value") < 0.8, "High")
                .when(col("avg_value") < 1.0, "Medium")
                .otherwise("Low")) \
    .withColumn("trend", 
                when(col("max_value") - col("min_value") > col("avg_value") * 0.5, "Volatile")
                .when(col("max_value") - col("min_value") > col("avg_value") * 0.2, "Stable")
                .otherwise("Very Stable"))

print("Data merge and standardization completed")
print("Merged data rows:", combined_silver.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Patient Summary Table

# COMMAND ----------

# Patient summary data
patient_summary = combined_silver \
    .groupBy("patient_id") \
    .agg(
        countDistinct("itemid").alias("unique_measurements"),
        count("itemid").alias("total_measurements"),
        avg("avg_quality_score").alias("avg_quality_score"),
        min("date").alias("first_measurement_date"),
        max("date").alias("last_measurement_date"),
        countDistinct("date").alias("measurement_days")
    ) \
    .withColumn("measurement_frequency", 
                col("total_measurements") / col("measurement_days")) \
    .withColumn("patient_risk_score", 
                when(col("avg_quality_score") < 0.7, 3)
                .when(col("avg_quality_score") < 0.9, 2)
                .otherwise(1)) \
    .withColumn("etl_timestamp", current_timestamp())

print("Patient summary table creation completed")
print("Patient count:", patient_summary.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Time Series Data

# COMMAND ----------

# Time series data - aggregated by date
daily_summary = combined_silver \
    .groupBy("date", "itemid", "item_name") \
    .agg(
        count("patient_id").alias("patient_count"),
        avg("avg_value").alias("avg_value"),
        stddev("avg_value").alias("std_value"),
        count("risk_level").alias("risk_count")
    ) \
    .withColumn("etl_timestamp", current_timestamp())

print("Time series data creation completed")
print("Time series data rows:", daily_summary.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Save Silver Layer Data

# COMMAND ----------

# Save merged data to Silver layer
combined_silver.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/FileStore/icu-data/silver/silver_combined_measurements")

print("Merged data saved to Silver layer completed")

# Save patient summary data to Silver layer
patient_summary.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/FileStore/icu-data/silver/silver_patient_summary")

print("Patient summary data saved to Silver layer completed")

# Save time series data to Silver layer
daily_summary.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/FileStore/icu-data/silver/silver_daily_summary")

print("Time series data saved to Silver layer completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Data Quality Check

# COMMAND ----------

def silver_data_quality_check():
    """Silver layer data quality check"""
    
    print("=== Silver Layer Data Quality Check ===")
    
    # Check Silver layer files
    silver_files = dbutils.fs.ls("/FileStore/icu-data/silver/")
    print("Silver layer file count:", len(silver_files))
    for file in silver_files:
        print("  -", file.name)
    
    # Data statistics
    print("\nData statistics:")
    print("Merged data rows:", combined_silver.count())
    print("Patient summary rows:", patient_summary.count())
    print("Time series rows:", daily_summary.count())
    
    # Check risk distribution
    print("\nRisk distribution:")
    combined_silver.groupBy("risk_level").count().show()
    
    # Check trend distribution
    print("\nTrend distribution:")
    combined_silver.groupBy("trend").count().show()
    
    # Check patient risk score distribution
    print("\nPatient risk score distribution:")
    patient_summary.groupBy("patient_risk_score").count().show()

# Execute data quality check
silver_data_quality_check()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Silver Layer ETL Completion Summary

# COMMAND ----------

print("Silver layer ETL processing completed!")
print("Data rows - Merged:", combined_silver.count(), "Patients:", patient_summary.count())
print("Next: Run Gold layer ETL notebook")
