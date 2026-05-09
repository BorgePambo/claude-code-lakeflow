# Northwind Lakeflow Declarative Pipelines (SDP)

**Production-Ready Medallion Architecture for Databricks**

---

## 📋 Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Directory Structure](#directory-structure)
4. [Getting Started](#getting-started)
5. [Layer Details](#layer-details)
6. [CDC and SCD2 Strategy](#cdc-and-scd2-strategy)
7. [Data Quality](#data-quality)
8. [Execution Guide](#execution-guide)
9. [Best Practices](#best-practices)
10. [Troubleshooting](#troubleshooting)

---

## Overview

This is a **production-grade** data pipeline built on **Databricks Lakeflow Declarative Pipelines (SDP)** that implements a modern medallion architecture for the Northwind database.

### Key Characteristics

- ✅ **Modern Databricks Architecture**: Uses `from pyspark import pipelines as dp` (no legacy DLT)
- ✅ **Medallion Pattern**: Bronze → Silver_Clean → Silver_Dimensional → Gold
- ✅ **SCD Type 2**: Automatic slowly-changing dimension tracking
- ✅ **Auto CDC**: Modern change data capture for incremental processing
- ✅ **Data Quality**: Built-in expectations and validation
- ✅ **Star Schema**: Optimized for BI and analytics
- ✅ **Modular Design**: Reusable functions and clear separation of concerns
- ✅ **Production Ready**: Error handling, logging, and monitoring

### Technology Stack

```
Databricks Lakeflow Declarative Pipelines (SDP)
├─ PySpark 3.5+
├─ Python 3.11+
├─ Delta Lake 3.0+
├─ Unity Catalog
└─ Databricks SQL
```

---

## Architecture

### Medallion Layers

```
BRONZE LAYER
│ Raw data from source systems
│ 7 tables: categories, products, customers, employees, orders, order_details, shippers
└─ No transformations, direct ingestion

         ↓

SILVER_CLEAN LAYER
│ Data standardization and quality
│ 7 clean tables
│ ├─ Column name standardization (snake_case)
│ ├─ Type casting and validation
│ ├─ Null handling
│ ├─ Duplicate removal
│ ├─ Audit columns (_loaded_at, _source, _processing_timestamp)
│ └─ Change tracking hashes (_change_hash)

         ↓

SILVER_DIMENSIONAL LAYER
│ Dimensional modeling (Star schema)
│ ├─ 4 Dimensions (SCD Type 2): customers, products, employees, shippers
│ ├─ 1 Date Dimension: temporal analysis
│ ├─ 1 Fact Table: sales transactions
│ ├─ Surrogate keys for all dimensions
│ ├─ Foreign keys in fact table
│ └─ CDC-ready structure

         ↓

GOLD LAYER
│ Analytics and BI aggregations
│ 6 tables optimized for consumption
│ ├─ gold_sales_by_country: Geographic analysis
│ ├─ gold_sales_by_category: Product category performance
│ ├─ gold_sales_by_employee: Employee sales rankings
│ ├─ gold_top_products: Top 50 products
│ ├─ gold_monthly_trends: Time-series trends
│ └─ gold_kpis: Executive KPIs
```

---

## Directory Structure

```
c:\DATABRICKS\claude-code-lakeflow\
├── README.md                          ← This file
├── pipeline_diagram.md                ← Visual architecture
└── transformations/
    ├── utils.py                       ← Shared utility functions
    ├── bronze.py                      ← Bronze layer (reference only)
    ├── silver_clean.py                ← Data cleaning layer
    ├── silver_dimensional.py          ← Dimensional modeling
    └── gold.py                        ← Analytics aggregations

Database Catalog Structure:

northwind (catalog)
├── bronze (schema)
│   ├── categories
│   ├── products
│   ├── customers
│   ├── employees
│   ├── orders
│   ├── order_details
│   └── shippers
├── silver_clean (schema)
│   ├── categories_clean
│   ├── products_clean
│   ├── customers_clean
│   ├── employees_clean
│   ├── orders_clean
│   ├── order_details_clean
│   └── shippers_clean
├── silver_dimensional (schema)
│   ├── dim_customers       (SCD Type 2)
│   ├── dim_products        (SCD Type 2)
│   ├── dim_employees       (SCD Type 2)
│   ├── dim_shippers        (Master)
│   ├── dim_dates           (Master)
│   └── fact_sales          (Transactions)
└── gold (schema)
    ├── gold_sales_by_country
    ├── gold_sales_by_category
    ├── gold_sales_by_employee
    ├── gold_top_products
    ├── gold_monthly_trends
    └── gold_kpis
```

---

## Getting Started

### Prerequisites

- **Databricks Workspace** (Unity Catalog enabled)
- **Python 3.11+**
- **PySpark 3.5+**
- **Northwind database** with bronze tables already created

### Setup Steps

#### 1. Clone/Import Repository

```bash
# In Databricks Workspace
git clone https://github.com/your-org/northwind-lakeflow.git
cd northwind-lakeflow
```

#### 2. Create Schemas

```sql
-- In Databricks SQL
CREATE SCHEMA IF NOT EXISTS northwind.silver_clean
  COMMENT = 'Standardized and validated data';

CREATE SCHEMA IF NOT EXISTS northwind.silver_dimensional
  COMMENT = 'Dimensional models and fact tables';

CREATE SCHEMA IF NOT EXISTS northwind.gold
  COMMENT = 'Analytics and BI aggregations';
```

#### 3. Initialize Checkpoints

```bash
# Create checkpoint directories for incremental processing
mkdir -p /tmp/checkpoints/silver_clean
mkdir -p /tmp/checkpoints/silver_dimensional
mkdir -p /tmp/checkpoints/gold
```

#### 4. Deploy Pipeline

```bash
# In Databricks Workspace using Workflows

# Create three workflow jobs:
# 1. silver_clean_daily (scheduled daily)
# 2. silver_dimensional_weekly (scheduled weekly)
# 3. gold_refresh_hourly (scheduled hourly)
```

---

## Layer Details

### 🔵 SILVER_CLEAN Layer

**Purpose**: Standardization and data quality

**Input**: Bronze raw tables
**Output**: 7 clean tables in `northwind.silver_clean`

**Transformations Applied**:

| Transformation | Details |
|---|---|
| Column Standardization | Convert to snake_case, remove special chars |
| Type Casting | Enforce correct data types (int, decimal, date) |
| Null Handling | Apply null strategy: defaults or placeholders |
| String Normalization | Trim whitespace, convert to proper case |
| Duplicate Removal | Remove duplicates based on primary keys |
| Audit Columns | Add _loaded_at, _source, _processing_timestamp |
| Change Hash | Create _change_hash for CDC tracking |

### 📊 SILVER_DIMENSIONAL Layer

**Purpose**: Dimensional modeling and star schema

**Tables Created**:

**Dimensions (with SCD Type 2)**:
- `dim_customers`: Customer master data
- `dim_products`: Product + Category information
- `dim_employees`: Employee + Manager hierarchy
- `dim_shippers`: Shipping carrier master
- `dim_dates`: Temporal dimension

**Fact Table**:
- `fact_sales`: Sales transactions (grain: order × product)

### ⭐ GOLD Layer

**Purpose**: Analytics-ready aggregations for BI

**Output**: 6 aggregated tables in `northwind.gold`

| Table | Aggregation | Key Metrics |
|---|---|---|
| **gold_sales_by_country** | By country | total_sales, total_orders, avg_order_value |
| **gold_sales_by_category** | By product category | sales, quantity, revenue_contribution |
| **gold_sales_by_employee** | By employee | sales, ranking, territory performance |
| **gold_top_products** | Top 50 products | rank_by_sales, rank_by_quantity |
| **gold_monthly_trends** | By month | monthly_sales, 3m_moving_avg, MoM_change |
| **gold_kpis** | Executive summary | total_sales, customer_count, KPIs |

---

## CDC and SCD2 Strategy

### Change Data Capture (CDC)

**Modern Databricks CDC Approach**:

```
Auto CDC Pipeline (fact_sales):
├─ Source: fact_sales incremental changes
├─ Detection: _change_hash comparison
├─ Mode: Incremental upsert with checkpoints
└─ Result: Only changed records processed
```

### Slowly Changing Dimension Type 2

**SCD Type 2 tracks all changes**:

```
Timeline: _effective_date, _end_date, _is_current

Analytical Benefits:
├─ Historical analysis
├─ Period-over-period comparisons
├─ Change attribution
└─ Audit trail
```

---

## Data Quality

### Quality Framework

**Expectations Applied** in silver_clean layer:

```
1. NOT NULL Checks
   └─ All ID columns must be NOT NULL
   
2. Type Validation
   └─ Columns match declared types
   
3. Business Rule Checks
   └─ unitPrice >= 0, quantity >= 0
   
4. Freshness Checks
   └─ _loaded_at within last 24 hours
   
5. Duplicate Checks
   └─ No duplicates on primary keys
   
6. Referential Integrity
   └─ Foreign keys exist in related tables
```

---

## Execution Guide

### Running the Pipeline

#### Option 1: Databricks Workflows (Recommended)

```bash
# Step 1: Create workflow jobs in Databricks UI
# Workspace → Workflows → New Job

# Job 1: SILVER_CLEAN_DAILY
├─ Source: transformations/silver_clean.py
├─ Schedule: Daily at 01:00 UTC

# Job 2: SILVER_DIMENSIONAL_WEEKLY
├─ Source: transformations/silver_dimensional.py
├─ Schedule: Weekly, Sunday 02:00 UTC
├─ Depends on: SILVER_CLEAN_DAILY

# Job 3: GOLD_REFRESH_HOURLY
├─ Source: transformations/gold.py
├─ Schedule: Hourly
├─ Depends on: SILVER_DIMENSIONAL_WEEKLY
```

#### Option 2: Notebook Execution

```python
# In Databricks Notebook
%run /Repos/northwind-lakeflow/transformations/silver_clean
%run /Repos/northwind-lakeflow/transformations/silver_dimensional
%run /Repos/northwind-lakeflow/transformations/gold
```

---

## Best Practices

### 1. Code Organization

✅ **Do**:
```python
# Modular functions with single responsibility
def create_hash_column(df, columns, hash_column_name):
    # One function, one purpose

# Clear naming conventions
dim_customers  # 'dim_' prefix for dimensions
fact_sales     # 'fact_' prefix for facts
gold_kpis      # 'gold_' prefix for gold layer
```

### 2. Performance

✅ **Do**:
```python
# Broadcast small tables
df_dim = spark.broadcast(dim_products)

# Use column selection early
df = df.select("id", "name", "amount")

# Partition on frequently filtered columns
.partitionBy(year("order_date"), month("order_date"))
```

### 3. Error Handling

✅ **Do**:
```python
# Validate inputs
validate_required_columns(df, ["id", "name"])

# Log events
log_pipeline_event("START", "products_clean", "RUNNING")
```

---

## Troubleshooting

### Common Issues and Solutions

#### Issue 1: "Table not found" Error

**Solution**:
```sql
-- Check if schema exists
SHOW DATABASES;
-- Create if missing
CREATE SCHEMA northwind.silver_clean;
```

#### Issue 2: Null Pointer Exception in Join

**Solution**:
```python
# Remove nulls before join
df_clean = df.filter(col("customer_id").isNotNull())
```

#### Issue 3: Out of Memory Error

**Solution**:
```python
# Repartition larger operations
df_large.repartition(100).join(df_small, ...)
```

---

## Support

- 📖 **Documentation**: See `pipeline_diagram.md` for visual architecture
- 🐛 **Issues**: Report bugs with logs and minimal reproduction case

---

## Version History

- **v1.0.0** (May 2024): Initial release with medallion architecture

---

**Ready to run on Databricks!** 🚀

For deployment, follow the [Execution Guide](#execution-guide) above.