# Pharmaceutical Data Pipeline

An end-to-end pharmaceutical data engineering pipeline built with Python, MongoDB, and a modern Data Lake architecture (Raw → Silver → Gold).  
This repository is designed as a **portfolio-grade project**, showcasing real-world data engineering practices from ingestion to analytics-ready outputs.

---

## Project Summary

**Objective**  
Transform large-scale pharmaceutical datasets from Kaggle into clean, normalized, and analytics-ready Parquet tables suitable for BI tools such as Power BI.

**Key Capabilities**
- End-to-end ETL pipeline  
- NoSQL-based raw data storage (MongoDB)  
- Data Lake layering (Raw, Silver, Gold)  
- Robust data cleaning and deduplication  
- Optimized Parquet outputs for analytics  

---

## Data Sources

- **11000 Medicine Details**  
  Structured metadata about medicines, ingredients, dosage, and manufacturers.
- **250K Medicines Usage, Side Effects, and Substitutes**  
  Large-scale dataset covering drug usage patterns and reported side effects.

Source: Kaggle

---

## Architecture

Kaggle Datasets (CSV)  
↓  
Raw Layer (`data/raw/`)  
↓  
MongoDB (raw collections)  
↓  
Silver Layer (`datalake/silver/` – cleaned Parquet)  
↓  
Gold Layer (`datalake/gold/` – analytics-ready Parquet)  
↓  
Power BI / Analytics Tools

---

## Technology Stack

- Python  
- MongoDB  
- Pandas, NumPy  
- Kaggle API  
- Parquet (pyarrow / fastparquet)  
- Power BI (consumption layer)  

---

## Project Structure

pharmaceutical-data-pipeline/  
├── data/  
│   └── raw/                    — Raw CSV datasets (not committed)  
├── datalake/  
│   ├── silver/                 — Cleaned & normalized Parquet files  
│   └── gold/                   — Final analytics-ready Parquet  
├── scripts/  
│   ├── Bigdata.py  — Kaggle → MongoDB ingestion  
│   └── ETL.py— MongoDB → Silver & Gold transformation    
└── README.md  

---

## Pipeline Workflow

### 1. Ingestion (Kaggle → MongoDB)
- Authenticate using Kaggle API  
- Download and extract CSV datasets  
- Normalize column names  
- Insert raw data into MongoDB collections  

### 2. Transformation (MongoDB → Silver)
- Extract collections into DataFrames  
- Standardize drug names  
- Handle missing values and duplicates  
- Normalize side effects into structured lists  
- Save cleaned datasets as Parquet  

### 3. Serving (Gold Layer)
- Build unified pharmaceutical catalog  
- Aggregate side effects per drug  
- Apply final schema validation  
- Save analytics-ready Parquet table  

---

## Output Datasets

- medicine_details_cleaned.parquet  
- medicine_dataset_cleaned.parquet  
- gold_meds_combined.parquet  

---

## How to Run

python scripts/Bigdata.py  
python scripts/ETL.py  

---

## Use Cases

- Pharmaceutical analytics  
- Drug side-effect analysis  
- Healthcare BI dashboards  
- Data engineering portfolio demonstration  

---

## Portfolio Value

This project demonstrates:
- Practical handling of real-world datasets (250K+ records)  
- Industry-standard Data Lake design  
- Clean, reproducible ETL pipelines  
- Analytics-focused data modeling  

---

## Author

**Rayyan**  
Data Engineering & Analytics Enthusiast
