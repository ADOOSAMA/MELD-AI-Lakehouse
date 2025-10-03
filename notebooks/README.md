# Databricks ETL Notebooks

This folder contains the core ETL notebooks for the Azure Databricks Lakehouse project.

## Core Files

### 1. Data Setup
- **`00_workspace_setup_full_data.py`** - Workspace setup and complete data creation
  - Create project directory structure
  - Generate 200 rows of monitoring data + 150 rows of laboratory data
  - Create mapping data
  - Save to Raw layer

### 2. Bronze Layer ETL
- **`01_bronze_etl_full_data.py`** - Bronze layer data cleaning
  - Read complete data from Raw layer
  - Data cleaning and standardization
  - Add data quality scores
  - Save to Bronze layer

### 3. Silver Layer ETL
- **`02_silver_etl_fixed.py`** - Silver layer data aggregation
  - Read data from Bronze layer
  - Aggregate by patient and time window
  - Add risk scoring and trend analysis
  - Create patient summary table
  - Save to Silver layer

### 4. Gold Layer ETL
- **`03_gold_etl_fixed.py`** - Gold layer data analysis and MELD scoring
  - Read data from Silver layer
  - Calculate MELD scores
  - Create risk stratification
  - Generate patient profiles
  - Create clinical decision support
  - Save to Gold layer

## Usage Steps

1. **Run Workspace Setup**
   ```
   00_workspace_setup_full_data.py
   ```

2. **Run Bronze Layer ETL**
   ```
   01_bronze_etl_full_data.py
   ```

3. **Run Silver Layer ETL**
   ```
   02_silver_etl_fixed.py
   ```

4. **Run Gold Layer ETL**
   ```
   03_gold_etl_fixed.py
   ```

## Data Flow

```
Raw Layer → Bronze Layer → Silver Layer → Gold Layer
    ↓           ↓            ↓            ↓
Raw Data    Cleaned Data  Aggregated Data  Analyzed Data
```

## Technical Features

- **Complete Data Volume**: 200 rows of monitoring data + 150 rows of laboratory data
- **Data Integrity**: Verify data reading and saving integrity
- **Error Handling**: Includes complete error handling and data validation

## Important Notes

- All files have been tested and can run normally
- Uses complete data volume to ensure ETL processes have sufficient data to process
- Completely avoids AssertionError issues
- Data has been saved to the respective data layers

## Next Steps

1. Run all ETL notebooks
2. Set up Power BI connection
3. Create dashboards
4. Deploy to production environment
