# Modern Data Lakehouse Pipeline on Azure Databricks

## Project Overview
This project demonstrates an end-to-end modern data lakehouse pipeline implemented on Azure Databricks using the AdventureWorks dataset. It follows a layered architecture comprising Bronze (raw), Silver (refined), and Gold (analytics-ready) zones, leveraging key data engineering concepts such as Change Data Capture (CDC), Slowly Changing Dimensions (SCD), Delta Live Tables (DLT), Unity Catalog, and star schema modeling.

The pipeline orchestrates data ingestion, transformation, and modeling stages to build a scalable, governed, and performant analytics platform — blending the best of data lakes and data warehouses.

---

## Architecture Layers

- **Source / Bronze Layer**  
  Raw data ingested from AdventureWorks Parquet files stored in Azure Data Lake Storage Gen2. CDC is applied to capture incremental changes and store data in Delta format.

- **Silver Layer**  
  Data transformation and cleansing occur here. Related tables such as Product, ProductCategory, and ProductSubcategory are joined into unified tables. Data is stored in Delta format and managed under Unity Catalog schemas.

- **Gold Layer (Data Warehouse Layer)**  
  Star schema design with dimensional tables (`dimCustomer`, `dimProduct`, `dimTerritory`) and a fact table (`factOrders`). Slowly Changing Dimensions (SCD) logic is applied to maintain historical data. This layer supports efficient analytics and reporting.

---

## Architecture Diagram

---

## Workflow Configuration

This project uses a fully orchestrated Databricks job pipeline defined in [`databricks_job_workflow.json`](./workflow_orchestration/databricks_job_workflow.json). The pipeline coordinates the execution of all notebooks across Bronze, Silver, and Gold layers, using a parameterized and modular workflow.

---

## Key Features

- **Change Data Capture (CDC):** Incremental data processing to handle updates efficiently.
- **Slowly Changing Dimensions (SCD):** Maintains historical changes in dimension tables.
- **Delta Live Tables (DLT):** Automated pipeline for reliable data transformations.
- **Unity Catalog:** Centralized data governance and security.
- **Star Schema Modeling:** Optimized structure for analytical queries.
- **Workflow Orchestration:** One-click pipeline execution with parameterized notebooks.

---

## Technologies Used

- Azure Databricks (Apache Spark, Delta Lake, Delta Live Tables)
- Azure Data Lake Storage Gen2
- Unity Catalog for data governance
- Azure Databricks Workflows for orchestration
- Parquet file format for raw data storage
- AdventureWorks sample dataset

---

## Folder Structure in Data Lake

- /source - Raw Parquet files
- /bronze - Raw data in Delta format after CDC
- /silver - Transformed, cleansed data and joined tables
- /gold - Star schema tables for analytics (fact and dimension tables)

---

## Summary

This project showcases an enterprise-grade, scalable, and maintainable data lakehouse solution. It highlights best practices in modern data engineering by combining robust data ingestion, transformation, and modeling techniques, making it ideal for real-world business analytics and reporting scenarios.

---

