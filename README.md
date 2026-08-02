# Pizza Sales Data Pipeline: Medallion Architecture on Databricks

A complete data engineering pipeline built on Databricks using Apache Spark (PySpark) and Delta Lake. This project implements the **Medallion Architecture (Bronze, Silver, Gold)** to ingest, clean, join, and transform multi-source sales data into a self-updating business intelligence dashboard.

---

## 🚀 Project Overview & Objectives

The goal of this project is to build an automated data pipeline combining two different data sources to answer two primary business questions[cite: 1]:
1. **Objective 1:** Which pizza sells the most? (Best-selling pizzas by total quantity and revenue)[cite: 1]
2. **Objective 2:** Which day of the week generates the most revenue?[cite: 1]

---

## 🛠️ Tech Stack & Architecture

* **Platform:** Databricks (Free / Serverless Edition)[cite: 1]
* **Processing Engine:** Apache Spark (PySpark)[cite: 1]
* **Storage Pattern:** Delta Lake / Medallion Architecture[cite: 1]
* **Data Sources:**
  * **Database (Relational Reference):** Menu items (`pizzas.csv`, `pizza_types.csv`)[cite: 1, 2]
  * **Object Store (Transactional Logs):** Sales records (`orders.csv`, `order_details.csv`) stored in Databricks Volumes[cite: 1, 2]

---

## 🔄 Data Pipeline Flow

1. **Bronze Layer (Raw Landing):** Ingests raw CSV files from both the menu and object store folders using strict explicit schemas, saving them as untouched Delta tables[cite: 1].
2. **Silver Layer (Cleaned & Joined):** Normalizes mixed date formats (`YYYY-MM-DD` and `M/D/YYYY`), joins order transactions with relational menu details, computes line-item revenues, and filters anomalies[cite: 1].
3. **Gold Layer (Business Aggregations):** Aggregates clean data into final analytical tables (`gold_best_sellers`, `gold_revenue_by_day`) to power dashboard visualisations[cite: 1].

---

## ⚡ Key Feature: Automated Updates

The pipeline features a folder-level ingestion mechanism. Dropping new transactional files into the object store source folder allows the pipeline to automatically re-ingest, clean, and aggregate data—updating the dashboard with **zero code modifications**[cite: 1].

---

## 📁 Repository File Structure

```text
databricks-pizza-sales-pipeline/
│
├── README.md                                   # Repository documentation
├── 1_Project_Document_YogdeepG.docx            # Full project report and methodology[cite: 1]
│
├── 2_Notebook/
│   └── pizza_medallion_databricks.py           # PySpark Databricks notebook code[cite: 2]
│
├── 3_Data/
│   ├── menu/
│   │   ├── pizzas.csv                          # Menu reference data[cite: 1, 2]
│   │   └── pizza_types.csv                     # Menu type reference data[cite: 1, 2]
│   ├── orders/
│   │   └── orders.csv                          # Transactional orders log[cite: 1, 2]
│   └── order_details/
│       └── order_details.csv                   # Transactional order details[cite: 1, 2]
│
├── 4_AI_Prompts/
│   └── prompts.txt                             # AI prompts used during development[cite: 2]
│
├── 5_Citation/
│   └── data_source_citation.txt                # Dataset source references[cite: 1, 2]
│
└── 6_Screenshots/
    ├── CONFIG/                                 # Configuration screenshots[cite: 2]
    ├── OUTCOMES/                               # Iteration and result outputs[cite: 2]
    └── PIPELINE/                               # Bronze, Silver, and Gold code screenshots[cite: 2]
