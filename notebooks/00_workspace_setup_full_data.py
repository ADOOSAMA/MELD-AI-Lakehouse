# Databricks notebook source
# MAGIC %md
# MAGIC # Workspace Setup - Reasonable Data Volume Version
# MAGIC 
# MAGIC This notebook creates a reasonable amount of sample data to avoid Spark validation issues

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Project Initialization

# COMMAND ----------

# Create project directory structure
dbutils.fs.mkdirs("/FileStore/icu-data/raw/")
dbutils.fs.mkdirs("/FileStore/icu-data/bronze/")
dbutils.fs.mkdirs("/FileStore/icu-data/silver/")
dbutils.fs.mkdirs("/FileStore/icu-data/gold/")

print("Project directory structure created")
print("Raw layer: /FileStore/icu-data/raw/")
print("Bronze layer: /FileStore/icu-data/bronze/")
print("Silver layer: /FileStore/icu-data/silver/")
print("Gold layer: /FileStore/icu-data/gold/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Sample Data - Reasonable Data Volume

# COMMAND ----------

# Create data directly with reasonable data volume
import random
from datetime import datetime, timedelta

# Create monitoring data - reasonable data volume
chart_data = []
for i in range(50):  # 50 rows of data
    patient_id = f"P{10000 + i}"
    itemid = 220045  # Heart rate
    value = random.randint(60, 120)
    charttime = datetime.now() - timedelta(hours=random.randint(1, 168))
    
    chart_data.append({
        'patient_id': patient_id,
        'itemid': itemid,
        'item_name': 'Heart Rate',
        'value': str(value),
        'valuenum': float(value),
        'charttime': charttime.strftime("%Y-%m-%d %H:%M:%S"),
        'storetime': charttime.strftime("%Y-%m-%d %H:%M:%S"),
        'caregiver_id': f"CG{random.randint(1000, 9999)}",
        'warning': 0,
        'error': 0,
        'resultstatus': 'Final'
    })

# Create lab data - reasonable data volume
lab_data = []
for i in range(30):  # 30 rows of data
    patient_id = f"P{10000 + i}"
    itemid = 51265  # Hemoglobin
    # Avoid using round function, use integers directly
    value = random.randint(8, 16)
    charttime = datetime.now() - timedelta(hours=random.randint(1, 168))
    
    lab_data.append({
        'patient_id': patient_id,
        'itemid': itemid,
        'item_name': 'Hemoglobin',
        'value': str(value),
        'valuenum': float(value),
        'charttime': charttime.strftime("%Y-%m-%d %H:%M:%S"),
        'storetime': charttime.strftime("%Y-%m-%d %H:%M:%S"),
        'specimen_id': f"SP{random.randint(10000, 99999)}",
        'flag': 'N',
        'priority': 'Routine'
    })

# Create mapping data - reasonable data volume
mapping_data = [
    {'itemid': 220045, 'item_name': 'Heart Rate', 'category': 'Monitoring', 'unit': 'bpm', 'normal_range': '60-100'},
    {'itemid': 220046, 'item_name': 'Blood Pressure', 'category': 'Monitoring', 'unit': 'mmHg', 'normal_range': '90-140'},
    {'itemid': 220047, 'item_name': 'Temperature', 'category': 'Monitoring', 'unit': '°C', 'normal_range': '36.0-37.5'},
    {'itemid': 51265, 'item_name': 'Hemoglobin', 'category': 'Laboratory', 'unit': 'g/dL', 'normal_range': '12-16'},
    {'itemid': 51222, 'item_name': 'White Blood Cell Count', 'category': 'Laboratory', 'unit': '×10³/μL', 'normal_range': '4-10'},
    {'itemid': 51221, 'item_name': 'Platelet Count', 'category': 'Laboratory', 'unit': '×10³/μL', 'normal_range': '150-450'}
]

print("Sample data creation completed")
print("Monitoring data rows:", len(chart_data))
print("Lab data rows:", len(lab_data))
print("Mapping data rows:", len(mapping_data))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create CSV Files Directly - Completely Avoid DataFrame

# COMMAND ----------

# Create CSV files directly, completely avoid DataFrame
try:
    # Create monitoring data CSV
    chart_csv_content = "patient_id,itemid,item_name,value,valuenum,charttime,storetime,caregiver_id,warning,error,resultstatus\n"
    for row in chart_data:
        chart_csv_content += f"{row['patient_id']},{row['itemid']},{row['item_name']},{row['value']},{row['valuenum']},{row['charttime']},{row['storetime']},{row['caregiver_id']},{row['warning']},{row['error']},{row['resultstatus']}\n"
    
    # Create lab data CSV
    lab_csv_content = "patient_id,itemid,item_name,value,valuenum,charttime,storetime,specimen_id,flag,priority\n"
    for row in lab_data:
        lab_csv_content += f"{row['patient_id']},{row['itemid']},{row['item_name']},{row['value']},{row['valuenum']},{row['charttime']},{row['storetime']},{row['specimen_id']},{row['flag']},{row['priority']}\n"
    
    # Create mapping data CSV
    mapping_csv_content = "itemid,item_name,category,unit,normal_range\n"
    for row in mapping_data:
        mapping_csv_content += f"{row['itemid']},{row['item_name']},{row['category']},{row['unit']},{row['normal_range']}\n"
    
    # Save to Raw layer
    dbutils.fs.put("/FileStore/icu-data/raw/chartevents.csv", chart_csv_content, overwrite=True)
    dbutils.fs.put("/FileStore/icu-data/raw/labevents.csv", lab_csv_content, overwrite=True)
    dbutils.fs.put("/FileStore/icu-data/raw/itemid_to_variable_map.csv", mapping_csv_content, overwrite=True)
    
    print("CSV files created successfully")
    print("Data saved to Raw layer completed")
    
except Exception as e:
    print(f"CSV file creation failed: {e}")
    print("Error details:", str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify File Creation

# COMMAND ----------

# Verify if files were created successfully
try:
    raw_files = dbutils.fs.ls("/FileStore/icu-data/raw/")
    print("Raw layer file list:")
    for file in raw_files:
        print(f"  - {file.name}")
    
    print(f"\nRaw layer file count: {len(raw_files)}")
    
    # Show file content preview
    print("\nFile content preview:")
    for file in raw_files:
        if file.name.endswith('.csv'):
            content = dbutils.fs.head(file.path, maxBytes=1048576)  # 1MB
            print(f"\n{file.name}:")
            print(content[:300])  # Show first 300 characters
    
    # Count data rows
    print("\nData row count:")
    for file in raw_files:
        if file.name.endswith('.csv'):
            content = dbutils.fs.head(file.path, maxBytes=10485760)  # 10MB
            lines = content.strip().split('\n')
            print(f"{file.name}: {len(lines)} rows (including header)")
    
except Exception as e:
    print(f"File verification failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Workspace Setup Complete

# COMMAND ----------

print("Workspace setup completed!")
print("Project is ready to start ETL process")

print("\nProject structure:")
print("Raw layer: Raw data")
print("Bronze layer: Cleaned data")
print("Silver layer: Aggregated data")
print("Gold layer: Analysis data")

print("\nNext steps:")
print("1. Run Bronze layer ETL notebook")
print("2. Run Silver layer ETL notebook")
print("3. Run Gold layer ETL notebook")
print("4. Setup Power BI connection")

print("\nData statistics:")
print("Monitoring data rows:", len(chart_data))
print("Lab data rows:", len(lab_data))
print("Mapping data rows:", len(mapping_data))
