# 🏎️ Formula 1 Data Optimization Platform  
## Optimizing Formula 1 Data Processing Using Azure Databricks & Delta Lake

---

## 📌 Project Overview

This project implements a scalable, cloud-based data engineering pipeline to process and optimize historical Formula 1 racing datasets using Microsoft Azure services.

The system is designed to simulate a real-world enterprise data platform capable of handling structured data at scale, enforcing governance, and enabling high-performance analytics.

The architecture follows the **Medallion Architecture (Bronze → Silver → Gold)** to ensure structured transformation, reliability, and performance optimization.

---

## 🎯 Project Objectives

- Build a cloud-native ETL pipeline using Azure services
- Implement Delta Lake for ACID-compliant storage
- Optimize large-scale data transformations using Spark
- Enable incremental data loading with MERGE
- Enforce governance using Unity Catalog
- Apply performance tuning techniques such as partitioning and Z-Ordering
- Design a configuration-driven architecture for maintainability

---

## 🏗️ Architecture Overview

### 🔹 High-Level Flow

Raw F1 CSV Data  
→ Azure Data Factory (Orchestration)  
→ Azure Data Lake Storage Gen2 (Bronze Layer)  
→ Azure Databricks (Transformation & Optimization)  
→ Delta Lake Tables (Silver & Gold Layers)  
→ Analytical Queries & Reporting  

---

## 🧱 Architecture Components

### 1️⃣ Azure Data Factory (ADF)
- Pipeline orchestration
- Trigger-based execution
- Parameterized pipelines
- Automated ingestion into Bronze layer

### 2️⃣ Azure Data Lake Storage Gen2 (ADLS)
- Scalable cloud object storage
- Organized into:
  - Bronze (Raw)
  - Silver (Cleaned)
  - Gold (Aggregated)
- Managed using Unity Catalog external locations

### 3️⃣ Azure Databricks
- Distributed Spark processing
- Notebook-driven ETL transformations
- Incremental data processing
- Performance tuning via caching & partitioning

### 4️⃣ Delta Lake
- ACID transactions
- Schema enforcement & evolution
- Time Travel capability
- OPTIMIZE and ZORDER
- MERGE-based incremental loads

### 5️⃣ Unity Catalog
- Centralized governance
- Managed external storage locations
- Secure access control
- Metadata management

---

## 📊 Dataset Description

The dataset consists of historical Formula 1 data including:

- Circuits
- Drivers
- Constructors
- Races
- Results
- Lap Times
- Pit Stops
- Driver Standings
- Constructor Standings

The data is ingested in raw CSV format and transformed into optimized Delta tables.

---

## 🧠 Data Engineering Design Pattern

### 🔹 Medallion Architecture

**Bronze Layer**
- Raw ingested data
- Minimal transformation
- Schema applied
- Audit columns added

**Silver Layer**
- Data cleansing
- Null handling
- Standardized column formats
- Relational joins

**Gold Layer**
- Business-level aggregations
- Analytical tables
- Query-optimized structures

---

## ⚙️ Optimization Techniques Implemented

### ✅ 1. Incremental Data Loading
- MERGE INTO for upserts
- Avoids full table refresh
- Reduces compute costs

### ✅ 2. Partitioning Strategy
- Partitioned by race year and circuit
- Improves filter performance
- Reduces data scan size

### ✅ 3. Z-Ordering
Applied on frequently filtered columns such as:
- driver_id
- race_id

Improves query latency significantly.

### ✅ 4. Delta OPTIMIZE
- File compaction
- Reduced small file problem
- Improved read performance

### ✅ 5. VACUUM
- Storage cleanup
- Old file removal
- Cost optimization

### ✅ 6. Caching
- In-memory caching for frequently accessed tables
- Reduced repeated computation

---

## 📈 Sample Analytical Queries

### 🔹 Fastest Lap Per Race
```sql
SELECT race_id, driver_id, MIN(milliseconds) AS fastest_lap
FROM lap_times
GROUP BY race_id, driver_id;
