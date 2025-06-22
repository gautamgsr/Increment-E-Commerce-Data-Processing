# Increment-E-Commerce-Data-Processing
# 🛒 E-Commerce Data Processing Pipeline (End-to-End)

This project is a complete **end-to-end data engineering solution** for processing e-commerce data using **modern data warehousing practices**. It demonstrates the use of **Azure Databricks**, **Delta Lake**, and the **Medallion Architecture (Bronze, Silver, Gold)** to handle real-time and batch data workflows at scale.

---

## 🚀 Project Overview

The pipeline simulates a real-world e-commerce environment where raw data is ingested, cleaned, transformed, and modeled to support analytics and reporting needs.

---

## 🔧 Technologies Used

- **Azure Databricks**
- **Delta Lake**
- **Delta Live Tables (DLT)**
- **Apache Spark (PySpark)**
- **Azure Data Lake Storage**
- **Star Schema (Fact/Dimension modeling)**
- **Slowly Changing Dimensions (Type 1 & Type 2)**

---

## 🧱 Architecture: Medallion Pattern

This project is structured into **three data layers**:

### 1️⃣ Bronze Layer
- Raw data ingestion from Azure Data Lake.
- Minimal transformations (schema enforcement, partitioning).
- Stored as raw Delta tables.

### 2️⃣ Silver Layer
- Data cleansing, filtering, and joining.
- Applied business logic (deduplication, basic aggregations).
- Modeled into clean Delta tables.

### 3️⃣ Gold Layer
- Analytical and reporting layer.
- Created **Star Schema** with:
  - **Fact tables** (e.g., orders, transactions)
  - **Dimension tables** (e.g., customers, products, time)
- Implemented **Slowly Changing Dimensions**:
  - **Type 1:** Overwrites changes.
  - **Type 2:** Tracks historical changes with versioning and timestamps.

---

## 🧪 Key Features

- ✅ End-to-end pipeline automation using Delta Live Tables.
- ✅ Medallion architecture for structured, scalable data modeling.
- ✅ Robust data warehousing practices using **Star Schema**.
- ✅ SCD Type 1 & Type 2 for historical accuracy in dimension data.
- ✅ Designed for high performance and maintainability.

---
## 🙏 Acknowledgment

This project was built with guidance and inspiration from **Ansh Lamba’s YouTube tutorials**, which provided valuable insights into building modern data engineering solutions using Databricks and Delta Lake.

> Credit: [Ansh Lamba on YouTube](https://www.youtube.com/@AnshLamba)

---

## 📌 Author

**Gautam Rawat**  
Email: gautam.rawat3007@gmail.com  
LinkedIn: [linkedin.com/in/gautam-rawat-710029194](https://linkedin.com/in/gautam-rawat-710029194)





