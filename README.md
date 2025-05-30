# End-To-End-Azure-Data-Pipeline-for-Adventure-Data

# 🚀 GitHub Data Pipeline Project

This project demonstrates an end-to-end data engineering pipeline built using **Azure Data Factory**, **Azure Databricks**, **Azure Synapse Analytics**, and **Power BI**. The pipeline extracts data from the **GitHub API**, processes and transforms it through various layers (Bronze, Silver, Gold), and presents it using Power BI for visualization.

---

## 📌 Project Workflow

### 1. 🔄 Data Extraction (Bronze Layer)
- **Tool Used**: Azure Data Factory
- **Process**:
  - Connected to the GitHub API
  - Extracted repository data in JSON format
  - Stored raw data in **Azure Data Lake Storage Gen2** under the **Bronze layer**

### 2. 🧹 Data Transformation (Silver Layer)
- **Tool Used**: Azure Databricks (PySpark)
- **Process**:
  - Read raw JSON files from Bronze layer
  - Performed schema validation and cleansing
  - Flattened nested fields and applied necessary transformations
  - Stored cleaned data as **Parquet** files in the **Silver layer**

### 3. 🧮 Data Modeling (Gold Layer)
- **Tool Used**: Azure Synapse Analytics
- **Process**:
  - Loaded transformed Parquet files from Silver layer into Synapse tables
  - Applied business logic and aggregations
  - Final curated datasets stored in **Gold layer** tables (ready for analytics)

### 4. 📊 Data Visualization
- **Tool Used**: Power BI
- **Process**:
  - Connected Power BI to Azure Synapse (Gold layer)
  - Created dashboards for repository insights such as:
    - Stars, forks, issues
    - Top contributors
    - Commit trends and activity patterns

---

## 🧰 Tech Stack

- **Data Extraction**: Azure Data Factory
- **Storage**: Azure Data Lake Storage Gen2 (Bronze, Silver, Gold layers)
- **Transformation**: Azure Databricks (PySpark)
- **Data Warehouse**: Azure Synapse Analytics
- **Visualization**: Power BI

---

