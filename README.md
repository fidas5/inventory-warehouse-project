# 📊 Supply Chain Data Warehouse Project

## 🧠 Overview

This project implements a complete **Data Warehouse pipeline** for supply chain analysis, from raw data ingestion to interactive dashboard visualization.

The goal is to help decision-makers analyze inventory performance, reduce stockouts, optimize costs, and improve service levels.

---

## 📂 Data Source

The dataset used in this project comes from:

* **Inventory Replenishment Time Series Dataset (Kaggle)**
  This dataset simulates real-world supply chain operations with daily inventory, demand, and replenishment data.

### 🔹 Dataset Characteristics

* ~10,000 observations
* 20 products (SKUs)
* Daily time series data
* Includes:
  * Demand & forecast
  * Inventory levels
  * Replenishment policies
  * Costs & KPIs
  * External factors (promotion, holidays, price, weather)



Dataset Link : 
*  https://www.kaggle.com/datasets/williamsewell/inventory-replenishment-timeseries?select=inventory_replenishment_timeseries_10000.csv
---

## ⚙️ Tech Stack

* **Python (Pandas)** → Data cleaning & transformation (ETL)
* **Google BigQuery** → Cloud Data Warehouse
* **Power BI** → Data visualization & dashboards

---



## 🚀 How to Run the Project

### 1️⃣ Clone the repository

```bash
git clone <your-repo-url>
cd project
```

---

### 2️⃣ Create virtual environment

```bash
python -m venv .venv
```

**Activate it:**

Windows:

```bash
.venv\Scripts\activate
```

---

### 3️⃣ Install dependencies

```bash
pip install -r requirements.txt
```

---

### 4️⃣ Run ETL Pipeline

#### Step 1: Extract & Prepare Data

```bash
python scripts/02_extract_to_staging.py
```

#### Step 2: Create Dimensions

```bash
python scripts/03_create_dimensions.py
```

#### Step 3: Create Fact Table

```bash
python scripts/04_create_fact_table.py
```

---

## ☁️ Load into BigQuery

1. Create dataset:

```
inventory_dwh
```

2. Upload:

* dimension tables
* fact table

---

## 📊 Connect Power BI

* Get Data → Google BigQuery
* Select dataset
* Build dashboard visuals

---

## 📈 KPIs Implemented

* Fill Rate (Service Level)
* Stockout Rate (Risk)
* Total Cost (Economic Performance)
* Efficiency Score (Composite KPI)

---

## 🎯 Key Features

* Star schema modeling
* ETL pipeline in Python
* KPI-driven dashboards
* Policy comparison (minmax, base_stock, ml_reorder)

