# Databricks notebook source
# MAGIC %md
# MAGIC # Power BI Integration - Workspace Version
# MAGIC 
# MAGIC Set up Power BI connection, create semantic model and dashboards

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Power BI Related Libraries

# COMMAND ----------

# Install Power BI related libraries
%pip install powerbiclient

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Power BI Connection Configuration

# COMMAND ----------

# Power BI connection configuration
powerbi_config = {
    "workspace_id": "your-workspace-id",
    "dataset_id": "your-dataset-id",
    "client_id": "your-client-id",
    "client_secret": "your-client-secret",
    "tenant_id": "your-tenant-id"
}

print("Power BI connection configuration completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Semantic Model

# COMMAND ----------

# Create semantic model configuration
semantic_model_config = {
    "name": "ICU_Data_Semantic_Model",
    "tables": [
        {
            "name": "MELD_Scores",
            "source": "/FileStore/icu-data/gold/gold_meld_scores",
            "columns": [
                {"name": "patient_id", "type": "string"},
                {"name": "meld_score", "type": "double"},
                {"name": "risk_category", "type": "string"},
                {"name": "score_date", "type": "date"}
            ]
        },
        {
            "name": "Risk_Stratification",
            "source": "/FileStore/icu-data/gold/gold_risk_stratification",
            "columns": [
                {"name": "patient_id", "type": "string"},
                {"name": "risk_tier", "type": "string"},
                {"name": "monitoring_frequency", "type": "string"}
            ]
        },
        {
            "name": "Patient_Profiles",
            "source": "/FileStore/icu-data/gold/gold_patient_profiles",
            "columns": [
                {"name": "patient_id", "type": "string"},
                {"name": "age_group", "type": "string"},
                {"name": "care_complexity", "type": "string"}
            ]
        }
    ],
    "relationships": [
        {
            "from_table": "MELD_Scores",
            "from_column": "patient_id",
            "to_table": "Risk_Stratification",
            "to_column": "patient_id"
        },
        {
            "from_table": "Risk_Stratification",
            "from_column": "patient_id",
            "to_table": "Patient_Profiles",
            "to_column": "patient_id"
        }
    ]
}

print("Semantic model configuration completed")
print("Model name:", semantic_model_config["name"])
print("Table count:", len(semantic_model_config["tables"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create DAX Measures

# COMMAND ----------

# DAX measures configuration
dax_measures = {
    "Total_Patients": "COUNTROWS(MELD_Scores)",
    "High_Risk_Patients": "CALCULATE(COUNTROWS(MELD_Scores), MELD_Scores[risk_category] = \"High\")",
    "Average_MELD_Score": "AVERAGE(MELD_Scores[meld_score])",
    "Critical_Patients": "CALCULATE(COUNTROWS(MELD_Scores), MELD_Scores[risk_category] = \"High\")",
    "Risk_Distribution": "DISTINCTCOUNT(MELD_Scores[risk_category])",
    "Patient_Count_by_Tier": "COUNTROWS(Risk_Stratification)",
    "Complex_Care_Patients": "CALCULATE(COUNTROWS(Patient_Profiles), Patient_Profiles[care_complexity] = \"Complex\")"
}

print("DAX measures configuration completed")
print("Measure count:", len(dax_measures))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Dashboard Configuration

# COMMAND ----------

# Dashboard configuration
dashboard_config = {
    "name": "ICU_Data_Dashboard",
    "pages": [
        {
            "name": "Overview",
            "visuals": [
                {
                    "type": "Card",
                    "title": "Total Patients",
                    "measure": "Total_Patients"
                },
                {
                    "type": "Card",
                    "title": "High Risk Patients",
                    "measure": "High_Risk_Patients"
                },
                {
                    "type": "Card",
                    "title": "Average MELD Score",
                    "measure": "Average_MELD_Score"
                }
            ]
        },
        {
            "name": "Risk Analysis",
            "visuals": [
                {
                    "type": "Bar Chart",
                    "title": "Risk Distribution",
                    "x_axis": "risk_category",
                    "y_axis": "Total_Patients"
                },
                {
                    "type": "Pie Chart",
                    "title": "Risk Tier Distribution",
                    "category": "risk_tier",
                    "value": "Patient_Count_by_Tier"
                }
            ]
        },
        {
            "name": "Patient Profiles",
            "visuals": [
                {
                    "type": "Table",
                    "title": "Patient Summary",
                    "columns": ["patient_id", "meld_score", "risk_category", "age_group", "care_complexity"]
                },
                {
                    "type": "Scatter Chart",
                    "title": "MELD Score vs Risk",
                    "x_axis": "meld_score",
                    "y_axis": "risk_score"
                }
            ]
        }
    ]
}

print("Dashboard configuration completed")
print("Dashboard name:", dashboard_config["name"])
print("Page count:", len(dashboard_config["pages"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create Data Connection

# COMMAND ----------

# Create data connection configuration
data_connection_config = {
    "connection_type": "DirectQuery",
    "source": "Databricks",
    "connection_string": "Server=your-databricks-server;Database=your-database;",
    "authentication": "OAuth2",
    "refresh_schedule": "Daily"
}

print("Data connection configuration completed")
print("Connection type:", data_connection_config["connection_type"])
print("Data source:", data_connection_config["source"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Create Row-Level Security Configuration

# COMMAND ----------

# Row-level security configuration
rls_config = {
    "enabled": True,
    "rules": [
        {
            "table": "MELD_Scores",
            "filter": "MELD_Scores[patient_id] = USERNAME()"
        },
        {
            "table": "Risk_Stratification",
            "filter": "Risk_Stratification[patient_id] = USERNAME()"
        },
        {
            "table": "Patient_Profiles",
            "filter": "Patient_Profiles[patient_id] = USERNAME()"
        }
    ]
}

print("Row-level security configuration completed")
print("RLS enabled:", rls_config["enabled"])
print("Rule count:", len(rls_config["rules"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Create Power BI Integration Summary

# COMMAND ----------

print("Power BI integration configuration completed!")
print("Model:", semantic_model_config["name"], "Tables:", len(semantic_model_config["tables"]))
print("Dashboard:", dashboard_config["name"], "Measures:", len(dax_measures))
