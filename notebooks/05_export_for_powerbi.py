import os

try:
    gold_files = dbutils.fs.ls("/FileStore/icu-data/gold/")
    print("Gold layer file list:")
    for file in gold_files:
        print(f"  - {file.name}")
    
    # Check each file's content
    for file in gold_files:
        if file.name.endswith('.csv'):
            try:
                content = dbutils.fs.head(file.path, maxBytes=1048576)  # 1MB
                lines = content.strip().split('\n')
                print(f"{file.name}: {len(lines)} rows")
                if len(lines) > 0:
                    print(f"  First row: {lines[0][:100]}...")
            except Exception as e:
                print(f"{file.name}: Read failed - {e}")
                
except Exception as e:
    print(f"Check Gold layer data failed: {e}")
# Check Gold layer data
try:
    gold_files = dbutils.fs.ls("/FileStore/icu-data/gold/")
    print("Gold layer file list:")
    for file in gold_files:
        print(f"  - {file.name}")
    print(f"\nGold layer file count: {len(gold_files)}")
except Exception as e:
    print(f"Check Gold layer data failed: {e}")

# Export patient profile data
try:
    patient_profiles = spark.read.csv("/FileStore/icu-data/gold/gold_patient_profiles", header=True, inferSchema=True)
    print(f"Patient profile data: {patient_profiles.count()} rows")
    patient_profiles.coalesce(1).write.mode("overwrite").option("header", "true").csv("/FileStore/icu-data/export/patient_profiles.csv")
    print("Patient profile data export successful")
except Exception as e:
    print(f"Export patient profile data failed: {e}")

# Export MELD score data
try:
    meld_scores = spark.read.csv("/FileStore/icu-data/gold/gold_meld_scores", header=True, inferSchema=True)
    print(f"MELD score data: {meld_scores.count()} rows")
    meld_scores.coalesce(1).write.mode("overwrite").option("header", "true").csv("/FileStore/icu-data/export/meld_scores.csv")
    print("MELD score data export successful")
except Exception as e:
    print(f"Export MELD score data failed: {e}")

# Export risk stratification data
try:
    risk_stratification = spark.read.csv("/FileStore/icu-data/gold/gold_risk_stratification", header=True, inferSchema=True)
    print(f"Risk stratification data: {risk_stratification.count()} rows")
    risk_stratification.coalesce(1).write.mode("overwrite").option("header", "true").csv("/FileStore/icu-data/export/risk_stratification.csv")
    print("Risk stratification data export successful")
except Exception as e:
    print(f"Export risk stratification data failed: {e}")

# Export clinical decision support data
try:
    clinical_decision = spark.read.csv("/FileStore/icu-data/gold/gold_clinical_decision_support", header=True, inferSchema=True)
    print(f"Clinical decision data: {clinical_decision.count()} rows")
    clinical_decision.coalesce(1).write.mode("overwrite").option("header", "true").csv("/FileStore/icu-data/export/clinical_decision.csv")
    print("Clinical decision data export successful")
except Exception as e:
    print(f"Export clinical decision data failed: {e}")

# Export summary statistics data
try:
    summary_statistics = spark.read.csv("/FileStore/icu-data/gold/gold_summary_statistics", header=True, inferSchema=True)
    print(f"Summary statistics data: {summary_statistics.count()} rows")
    summary_statistics.coalesce(1).write.mode("overwrite").option("header", "true").csv("/FileStore/icu-data/export/summary_statistics.csv")
    print("Summary statistics data export successful")
except Exception as e:
    print(f"Export summary statistics data failed: {e}")

print("Data export completed! Files ready for Power BI import.")

# Re-export as single files
# Re-export as single files
print("Re-exporting as single files...")

# Export patient profile data
try:
    patient_profiles = spark.read.csv("/FileStore/icu-data/gold/gold_patient_profiles", header=True, inferSchema=True)
    print(f"Patient profile data: {patient_profiles.count()} rows")
    
    # Use coalesce(1) to ensure only one file
    patient_profiles.coalesce(1).write.mode("overwrite").option("header", "true").csv("/FileStore/icu-data/export/patient_profiles_single.csv")
    
    # Verify export
    files = dbutils.fs.ls("/FileStore/icu-data/export/patient_profiles_single.csv")
    for file in files:
        if file.name.endswith('.csv'):
            print(f"Export file: {file.path}")
            content = dbutils.fs.head(file.path)
            lines = content.strip().split('\n')
            print(f"File rows: {len(lines)}")
            print(f"First row: {lines[0][:100]}...")
    
    print("Patient profile data re-export successful")
except Exception as e:
    print(f"Re-export patient profile data failed: {e}")

# Export MELD score data
try:
    meld_scores = spark.read.csv("/FileStore/icu-data/gold/gold_meld_scores", header=True, inferSchema=True)
    print(f"MELD score data: {meld_scores.count()} rows")
    
    meld_scores.coalesce(1).write.mode("overwrite").option("header", "true").csv("/FileStore/icu-data/export/meld_scores_single.csv")
    
    files = dbutils.fs.ls("/FileStore/icu-data/export/meld_scores_single.csv")
    for file in files:
        if file.name.endswith('.csv'):
            print(f"Export file: {file.path}")
            content = dbutils.fs.head(file.path)
            lines = content.strip().split('\n')
            print(f"File rows: {len(lines)}")
            print(f"First row: {lines[0][:100]}...")
    
    print("MELD score data re-export successful")
except Exception as e:
    print(f"Re-export MELD score data failed: {e}")

# Export risk stratification data
try:
    risk_stratification = spark.read.csv("/FileStore/icu-data/gold/gold_risk_stratification", header=True, inferSchema=True)
    print(f"Risk stratification data: {risk_stratification.count()} rows")
    
    risk_stratification.coalesce(1).write.mode("overwrite").option("header", "true").csv("/FileStore/icu-data/export/risk_stratification_single.csv")
    
    files = dbutils.fs.ls("/FileStore/icu-data/export/risk_stratification_single.csv")
    for file in files:
        if file.name.endswith('.csv'):
            print(f"Export file: {file.path}")
            content = dbutils.fs.head(file.path)
            lines = content.strip().split('\n')
            print(f"File rows: {len(lines)}")
            print(f"First row: {lines[0][:100]}...")
    
    print("Risk stratification data re-export successful")
except Exception as e:
    print(f"Re-export risk stratification data failed: {e}")

# Export clinical decision data
try:
    clinical_decision = spark.read.csv("/FileStore/icu-data/gold/gold_clinical_decision_support", header=True, inferSchema=True)
    print(f"Clinical decision data: {clinical_decision.count()} rows")
    
    clinical_decision.coalesce(1).write.mode("overwrite").option("header", "true").csv("/FileStore/icu-data/export/clinical_decision_single.csv")
    
    files = dbutils.fs.ls("/FileStore/icu-data/export/clinical_decision_single.csv")
    for file in files:
        if file.name.endswith('.csv'):
            print(f"Export file: {file.path}")
            content = dbutils.fs.head(file.path)
            lines = content.strip().split('\n')
            print(f"File rows: {len(lines)}")
            print(f"First row: {lines[0][:100]}...")
    
    print("Clinical decision data re-export successful")
except Exception as e:
    print(f"Re-export clinical decision data failed: {e}")

# Export summary statistics data
try:
    summary_statistics = spark.read.csv("/FileStore/icu-data/gold/gold_summary_statistics", header=True, inferSchema=True)
    print(f"Summary statistics data: {summary_statistics.count()} rows")
    
    summary_statistics.coalesce(1).write.mode("overwrite").option("header", "true").csv("/FileStore/icu-data/export/summary_statistics_single.csv")
    
    files = dbutils.fs.ls("/FileStore/icu-data/export/summary_statistics_single.csv")
    for file in files:
        if file.name.endswith('.csv'):
            print(f"Export file: {file.path}")
            content = dbutils.fs.head(file.path)
            lines = content.strip().split('\n')
            print(f"File rows: {len(lines)}")
            print(f"First row: {lines[0][:100]}...")
    
    print("Summary statistics data re-export successful")
except Exception as e:
    print(f"Re-export summary statistics data failed: {e}")

print("Data re-export completed!")
