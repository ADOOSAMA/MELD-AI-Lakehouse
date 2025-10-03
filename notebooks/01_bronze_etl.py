# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer ETL - Full Data Version
# MAGIC 
# MAGIC Create Bronze layer data from Raw layer data, ensuring all data is read

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer ETL - Workspace Version
# MAGIC 
# MAGIC Create Bronze layer data from Raw layer data, perform data cleaning and standardization

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Raw Layer Data

# COMMAND ----------

# Read Raw layer data
chartevents_df = spark.read.csv("/FileStore/icu-data/raw/chartevents.csv", header=True, inferSchema=True)
labevents_df = spark.read.csv("/FileStore/icu-data/raw/labevents.csv", header=True, inferSchema=True)
itemid_mapping_df = spark.read.csv("/FileStore/icu-data/raw/itemid_to_variable_map.csv", header=True, inferSchema=True)

print("Raw layer data reading completed")
print("Monitoring data rows:", chartevents_df.count())
print("Lab data rows:", labevents_df.count())
print("Mapping data rows:", itemid_mapping_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Cleaning

# COMMAND ----------

from pyspark.sql.functions import *

# Monitoring data cleaning
chartevents_cleaned = chartevents_df \
    .filter(col("valuenum").isNotNull()) \
    .filter(col("valuenum") > 0) \
    .withColumn("data_type", lit("chart")) \
    .withColumn("etl_timestamp", current_timestamp()) \
    .withColumn("data_quality_score", 
                when(col("error") == 1, 0.5)
                .when(col("warning") == 1, 0.8)
                .otherwise(1.0)) \
    .withColumn("charttime_parsed", to_timestamp(col("charttime"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("storetime_parsed", to_timestamp(col("storetime"), "yyyy-MM-dd HH:mm:ss"))

print("Monitoring data cleaning completed")
print("Cleaned data rows:", chartevents_cleaned.count())

# Lab data cleaning
labevents_cleaned = labevents_df \
    .filter(col("valuenum").isNotNull()) \
    .filter(col("valuenum") > 0) \
    .withColumn("data_type", lit("lab")) \
    .withColumn("etl_timestamp", current_timestamp()) \
    .withColumn("data_quality_score", 
                when(col("flag").isin(["A", "B", "C", "D"]), 0.7)
                .otherwise(1.0)) \
    .withColumn("charttime_parsed", to_timestamp(col("charttime"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("storetime_parsed", to_timestamp(col("storetime"), "yyyy-MM-dd HH:mm:ss"))

print("Lab data cleaning completed")
print("Cleaned data rows:", labevents_cleaned.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Standardization

# COMMAND ----------

# Standardize monitoring data
chartevents_standardized = chartevents_cleaned \
    .withColumn("value_category", 
                when(col("valuenum") < 0.5, "Critical")
                .when(col("valuenum") < 0.8, "High")
                .when(col("valuenum") < 1.0, "Medium")
                .otherwise("Low")) \
    .withColumn("measurement_date", to_date(col("charttime_parsed"))) \
    .withColumn("measurement_hour", hour(col("charttime_parsed"))) \
    .withColumn("patient_risk_level", 
                when(col("data_quality_score") < 0.7, "High Risk")
                .when(col("data_quality_score") < 0.9, "Medium Risk")
                .otherwise("Low Risk"))

print("Monitoring data standardization completed")

# Standardize lab data
labevents_standardized = labevents_cleaned \
    .withColumn("value_category", 
                when(col("valuenum") < 0.5, "Critical")
                .when(col("valuenum") < 0.8, "High")
                .when(col("valuenum") < 1.0, "Medium")
                .otherwise("Low")) \
    .withColumn("measurement_date", to_date(col("charttime_parsed"))) \
    .withColumn("measurement_hour", hour(col("charttime_parsed"))) \
    .withColumn("patient_risk_level", 
                when(col("data_quality_score") < 0.7, "High Risk")
                .when(col("data_quality_score") < 0.9, "Medium Risk")
                .otherwise("Low Risk"))

print("Lab data standardization completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Save Bronze Layer Data

# COMMAND ----------

# Save monitoring data to Bronze layer
chartevents_standardized.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/FileStore/icu-data/bronze/bronze_chartevent")

print("Monitoring data saved to Bronze layer completed")

# Save lab data to Bronze layer
labevents_standardized.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/FileStore/icu-data/bronze/bronze_labevent")

print("Lab data saved to Bronze layer completed")

# Save mapping data to Bronze layer
itemid_mapping_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/FileStore/icu-data/bronze/bronze_itemid_mapping")

print("Mapping data saved to Bronze layer completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Quality Check

# COMMAND ----------

def bronze_data_quality_check():
    """Bronze layer data quality check"""
    
    print("=== Bronze Layer Data Quality Check ===")
    
    # Check Bronze layer files
    bronze_files = dbutils.fs.ls("/FileStore/icu-data/bronze/")
    print("Bronze layer file count:", len(bronze_files))
    for file in bronze_files:
        print("  -", file.name)
    
    # Data statistics
    print("\nData statistics:")
    print("Monitoring data rows:", chartevents_standardized.count())
    print("Lab data rows:", labevents_standardized.count())
    print("Mapping data rows:", itemid_mapping_df.count())
    
    # Check data quality scores
    print("\nData quality score distribution:")
    chartevents_standardized.groupBy("data_quality_score").count().show()
    labevents_standardized.groupBy("data_quality_score").count().show()
    
    # Check patient count
    print("\nPatient count:")
    print("Monitoring data patients:", chartevents_standardized.select("patient_id").distinct().count())
    print("Lab data patients:", labevents_standardized.select("patient_id").distinct().count())

# Execute data quality check
bronze_data_quality_check()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Bronze Layer ETL Completed

# COMMAND ----------

print("Bronze layer ETL processing completed!")
print("Data rows - Monitoring:", chartevents_standardized.count(), "Lab:", labevents_standardized.count())
print("Next: Run Silver layer ETL notebook")