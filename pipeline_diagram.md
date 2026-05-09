# Northwind Lakeflow Declarative Pipeline Architecture

## Medallion Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         NORTHWIND PIPELINE                          │
│                    Databricks Lakeflow (SDP)                        │
└─────────────────────────────────────────────────────────────────────┘

                                   │
                                   ▼
                    ╔═════════════════════════╗
                    │      BRONZE LAYER       │
                    │   (Raw/Unprocessed)     │
                    ╚═════════════════════════╝
                                   │
        ┌──────────┬──────────┬────┴────┬──────────┬──────────┐
        │          │          │         │          │          │
        ▼          ▼          ▼         ▼          ▼          ▼
    categories  products  customers employees   orders    shippers
    (Bronze)    (Bronze)   (Bronze)   (Bronze)   (Bronze)  (Bronze)
        │          │          │         │          │          │
        └──────────┴──────────┴────┬────┴──────────┴──────────┘
                                   │
                    Standardization & Data Quality
                       - Nulls handling
                       - Type casting
                       - Column standardization
                       - Duplicate removal
                       - Audit columns
                       - Change hashes
                                   │
                                   ▼
                    ╔═════════════════════════╗
                    │    SILVER_CLEAN LAYER   │
                    │   (Standardized Data)   │
                    ╚═════════════════════════╝
                                   │
        ┌──────────┬──────────┬────┴────┬──────────┬──────────┐
        │          │          │         │          │          │
        ▼          ▼          ▼         ▼          ▼          ▼
    categories  products  customers employees   orders    shippers
       _clean     _clean     _clean    _clean     _clean    _clean
                                   │
                    Dimensional Modeling
                       - Surrogate keys
                       - SCD Type 2
                       - Star schema joins
                       - Fact grain design
                                   │
                                   ▼
                    ╔═════════════════════════╗
                    │ SILVER_DIMENSIONAL LAYER│
                    │   (Star Schema Ready)   │
                    ╚═════════════════════════╝
                                   │
        ┌──────────────┬───────────┼──────────────┬──────────┐
        │              │           │              │          │
        ▼              ▼           ▼              ▼          ▼
    dim_customers  dim_products dim_employees dim_shippers dim_dates
    (SCD Type 2)   (SCD Type 2) (SCD Type 2)  (Master)    (Master)
        │              │           │              │          │
        └──────────────┴───────────┼──────────────┴──────────┘
                                   │
                    fact_sales
                    (Sales Transactions)
                    ├─ Grain: Order × Product
                    ├─ CDC-ready structure
                    └─ Foreign keys to dims
                                   │
                    Advanced Analytics
                       - Aggregations
                       - Rankings
                       - Time-series
                       - KPIs
                                   │
                                   ▼
                    ╔═════════════════════════╗
                    │      GOLD LAYER         │
                    │  (Analytics Ready)      │
                    ╚═════════════════════════╝
                                   │
        ┌──────────┬──────────┬────┴────┬──────────┬──────────┬──────────┐
        │          │          │         │          │          │          │
        ▼          ▼          ▼         ▼          ▼          ▼          ▼
  sales_by_    sales_by_  sales_by_  top_      monthly_    gold_
  country      category   employee   products   trends      kpis
   (Agg)        (Agg)      (Agg)     (Top 50)   (Trends)   (Metrics)
        │          │          │         │          │          │          │
        └──────────┴──────────┴────┬────┴──────────┴──────────┴──────────┘
                                   │
                ┌──────────────────┴──────────────────┐
                │                                     │
                ▼                                     ▼
           Power BI                        Databricks SQL
           Dashboards                      Queries & Analysis
```

---

## Data Flow Detailed

### BRONZE → SILVER_CLEAN

**Transformations:**
```
BRONZE              SILVER_CLEAN
┌──────────┐        ┌──────────────────────┐
│categories│──────→ │categories_clean      │
├──────────┤        ├──────────────────────┤
│products  │        │✓ Standardized names  │
├──────────┤        │✓ Type casted         │
│customers │        │✓ Nulls handled       │
├──────────┤        │✓ Duplicates removed  │
│employees │        │✓ Audit columns       │
├──────────┤        │✓ Change hash         │
│orders    │        │✓ Quality checks      │
├──────────┤        │✓ Normalized strings  │
│order_    │        └──────────────────────┘
│details   │
└──────────┘
```

---

### SILVER_CLEAN → SILVER_DIMENSIONAL

**Join Strategy:**
```
Dimensions:

┌─────────────────────────┐
│   dim_customers         │
├─────────────────────────┤
│ PK: dim_customer_sk     │
│ FK: customer_id (BK)    │
│ SCD Type 2 versioning   │
│ Attributes:             │
│ - companyname           │
│ - contactname           │
│ - city, country         │
└─────────────────────────┘

┌─────────────────────────┐
│   dim_products          │
├─────────────────────────┤
│ PK: dim_product_sk      │
│ FK: product_id (BK)     │
│ SCD Type 2 versioning   │
│ Join:                   │
│  products_clean         │
│  + categories_clean     │
│ Attributes:             │
│ - product_name          │
│ - category_name         │
│ - unit_price            │
└─────────────────────────┘

┌─────────────────────────┐
│   dim_employees         │
├─────────────────────────┤
│ PK: dim_employee_sk     │
│ FK: employee_id (BK)    │
│ SCD Type 2 versioning   │
│ Self-join: manager_name │
│ Attributes:             │
│ - title                 │
│ - reports_to_id         │
└─────────────────────────┘

┌─────────────────────────┐
│   dim_shippers          │
├─────────────────────────┤
│ PK: dim_shipper_sk      │
│ FK: shipper_id (BK)     │
│ Attributes:             │
│ - shipper_name          │
└─────────────────────────┘

┌─────────────────────────┐
│   dim_dates             │
├─────────────────────────┤
│ PK: date_key (YYYYMMDD) │
│ Attributes:             │
│ - full_date             │
│ - year, quarter, month  │
│ - day_of_week           │
│ - is_weekend            │
│ - is_holiday            │
└─────────────────────────┘


Fact Table:

┌──────────────────────────────────────────┐
│         fact_sales                       │
├──────────────────────────────────────────┤
│ PK: fact_sales_sk                        │
│ Grain: Order × Product                   │
│                                          │
│ Foreign Keys:                            │
│ ├─ customer_id → dim_customers          │
│ ├─ employee_id → dim_employees          │
│ ├─ product_id → dim_products            │
│ ├─ shipper_id → dim_shippers            │
│ └─ order_date → dim_dates                │
│                                          │
│ Measures:                                │
│ ├─ order_quantity                        │
│ ├─ unit_price                            │
│ ├─ discount_rate                         │
│ ├─ line_total                            │
│ ├─ discount_amount                       │
│ └─ freight                               │
│                                          │
│ Attributes:                              │
│ ├─ order_date                            │
│ ├─ required_date                         │
│ ├─ shipped_date                          │
│ └─ _is_deleted (CDC flag)                │
└──────────────────────────────────────────┘
```

---

### SILVER_DIMENSIONAL → GOLD

**Aggregation Strategy:**
```
GOLD LAYER TRANSFORMATIONS

fact_sales
    │
    ├─────────────→ gold_sales_by_country
    │               └─ GROUP BY: country
    │                 └─ Metrics: sales, orders, customers, avg_order_value
    │
    ├─────────────→ gold_sales_by_category
    │               └─ GROUP BY: category_name
    │                 └─ Metrics: sales, quantity, products, revenue_contribution
    │
    ├─────────────→ gold_sales_by_employee
    │               └─ GROUP BY: employee_id, employee_name, title
    │                 └─ Metrics: sales, orders, rank, territory
    │
    ├─────────────→ gold_top_products
    │               └─ GROUP BY: product_id, product_name, category
    │                 └─ TOP 50 by sales, quantity, popularity
    │
    ├─────────────→ gold_monthly_trends
    │               └─ GROUP BY: year_month
    │                 └─ Metrics: monthly_sales, mov_avg, MoM change, YoY
    │
    └─────────────→ gold_kpis
                    └─ Aggregate KPIs
                      └─ Metrics: total_sales, avg_order_value,
                                   customer_count, discount_impact
```

---

## Star Schema Diagram

```
                    ┌─────────────────┐
                    │   dim_dates     │
                    ├─────────────────┤
                    │ date_key        │
                    │ full_date       │
                    │ year, quarter   │
                    │ month, day_week │
                    │ is_weekend      │
                    └────────┬────────┘
                             │
                             │ order_date
                             │
          ┌──────────────────┼──────────────────┐
          │                  │                  │
          │                  ▼                  │
   ┌──────┴────────┐  ┌──────────────┐  ┌─────┴─────────┐
   │ dim_customers │  │  fact_sales  │  │ dim_employees │
   ├───────────────┤  ├──────────────┤  ├───────────────┤
   │customer_id (BK)  │fact_sales_sk │  │employee_id(BK)│
   │companyname    │  │orderid       │  │employee_name  │
   │contactname    │  │product_id →─┐│  │title          │
   │city, country  │  │customer_id→┐││  │manager_name   │
   │SCD2: dates    │  │employee_id→┐││  │SCD2: dates    │
   └───────────────┘  │shipper_id ──┼┼→─┤city, country  │
                      │             │└─→─└───────────────┘
                      │             │
                      │ Measures:   │
                      │ quantity    │
                      │ unit_price  │
                      │ discount    │
                      │ line_total  │
                      └──────┬──────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ dim_products    │
                    ├─────────────────┤
                    │ product_id (BK) │
                    │ product_name    │
                    │ category_name   │
                    │ unit_price      │
                    │ discontinued    │
                    │ SCD2: dates     │
                    └────────┬────────┘
                             │
                    (also related to)
                             │
                    ┌─────────────────┐
                    │ dim_shippers    │
                    ├─────────────────┤
                    │ shipper_id (BK) │
                    │ shipper_name    │
                    └─────────────────┘
```

---

## SCD Type 2 Implementation

**For: dim_customers, dim_products, dim_employees**

```
Versioning Columns:
├─ _effective_date: TIMESTAMP (when version becomes active)
├─ _end_date: TIMESTAMP (when version expires, NULL = current)
└─ _is_current: BOOLEAN (TRUE = current version)

Example: Customer name change

customer_id = 'ALFKI' changes name from "Alfreds Futterkiste" to "Alfreds Modified"

Version 1 (old):
├─ customer_id: ALFKI
├─ companyname: Alfreds Futterkiste
├─ _effective_date: 2024-01-01
├─ _end_date: 2024-06-15 ← Closed
├─ _is_current: FALSE

Version 2 (new):
├─ customer_id: ALFKI
├─ companyname: Alfreds Modified
├─ _effective_date: 2024-06-16
├─ _end_date: NULL ← Open (current)
├─ _is_current: TRUE
```

---

## CDC (Change Data Capture) Strategy

**For: fact_sales**

```
Incremental Processing:
├─ Auto CDC enabled
├─ Checkpoint-based deduplication
├─ _is_deleted flag for soft deletes
└─ Incremental merge patterns

Flow:
1. Extract changed records (delta.autoOptimize)
2. Upsert to fact_sales using _change_hash
3. Mark deleted records with _is_deleted = TRUE
4. Update dimension SCD2 versions as needed
```

---

## Data Quality Checks

**Applied in silver_clean layer:**

```
✓ Not Null Checks
  └─ All ID columns must be NOT NULL

✓ Freshness Checks
  └─ _loaded_at must be within 24 hours

✓ Row Count Checks
  └─ Each table must contain data

✓ Duplicate Checks
  └─ No duplicates on primary key

✓ Type Validation
  └─ Columns match expected data types

✓ Referential Integrity (checked in dimensional layer)
  └─ Foreign keys must exist in related tables

✓ Business Rules
  └─ unitPrice >= 0
  └─ quantity >= 0
  └─ discount <= 1.0
```

---

## Incremental Processing Architecture

```
Checkpoint Management:

/checkpoints/
├─ silver_clean/
│  ├─ categories_clean/
│  ├─ products_clean/
│  ├─ customers_clean/
│  ├─ employees_clean/
│  ├─ orders_clean/
│  ├─ order_details_clean/
│  └─ shippers_clean/
│
├─ silver_dimensional/
│  ├─ dim_customers/
│  ├─ dim_products/
│  ├─ dim_employees/
│  ├─ dim_shippers/
│  └─ fact_sales/
│
└─ gold/
   ├─ gold_sales_by_country/
   ├─ gold_sales_by_category/
   ├─ gold_sales_by_employee/
   ├─ gold_top_products/
   ├─ gold_monthly_trends/
   └─ gold_kpis/

Processing Strategy:
├─ Append mode: silver_clean
├─ Merge mode: silver_dimensional (SCD2)
├─ Upsert mode: fact_sales (CDC)
└─ Full refresh: gold layer (aggregations)
```

---

## Performance Considerations

```
Optimization Strategy:

1. CLUSTERING (on fact_sales)
   CLUSTER BY (customer_id, order_date)
   └─ Optimizes joins with dim_customers
   └─ Optimizes date-range queries

2. PARTITIONING (on fact_sales)
   PARTITION BY (year(order_date), month(order_date))
   └─ Enables partition pruning
   └─ Accelerates time-series queries

3. STATISTICS
   ANALYZE TABLE for optimizer hints
   └─ Column statistics
   └─ Join cardinality

4. CACHING
   ├─ Persist dimension tables in memory
   ├─ Materialize gold tables
   └─ Leverage Delta cache

5. Z-ORDERING
   Z-ORDER BY (customer_id, product_id, order_date)
   └─ Skips irrelevant data blocks
   └─ 5-10x query speedup on range filters
```

---

## Deployment Architecture

```
Databricks Environment:

Workspace Structure:
├─ /Repos/northwind-lakeflow/
│  ├─ bronze.py (reference only)
│  ├─ silver_clean.py
│  ├─ silver_dimensional.py
│  ├─ gold.py
│  └─ utils.py
│
├─ Unity Catalog:
│  └─ northwind (catalog)
│     ├─ bronze (schema)
│     │  └─ 7 source tables
│     ├─ silver_clean (schema)
│     │  └─ 7 clean tables
│     ├─ silver_dimensional (schema)
│     │  ├─ 5 dimensions
│     │  └─ 1 fact table
│     └─ gold (schema)
│        └─ 6 analytics tables
│
└─ Workflows:
   ├─ bronze_to_silver_daily (Scheduled)
   ├─ silver_dimensional_weekly (Scheduled)
   └─ gold_refresh_hourly (Scheduled)
```

---

## Dependencies and Execution Order

```
Execution DAG:

Phase 1: SILVER_CLEAN (Parallel)
├─ categories_clean ─┐
├─ products_clean   ├─→ silver_clean phase complete
├─ customers_clean  ├─┐
├─ employees_clean  ├─┤
├─ orders_clean     ├─┘
├─ order_details_clean
└─ shippers_clean

Phase 2: SILVER_DIMENSIONAL (Depends on Phase 1)
├─ dim_customers ─┐
├─ dim_products  ├─→ Dimensions ready
├─ dim_employees ├─┐
├─ dim_shippers  ├─┤
└─ dim_dates     ├─┘

Phase 3: FACT_SALES (Depends on Phase 1 & Phase 2)
└─ fact_sales ──→ Fact table ready

Phase 4: GOLD (Depends on Phase 3)
├─ gold_sales_by_country ─┐
├─ gold_sales_by_category ├─→ Analytics ready
├─ gold_sales_by_employee ├─┐
├─ gold_top_products      ├─┤
├─ gold_monthly_trends    ├─┘
└─ gold_kpis             ─┘
```

---

End of Pipeline Architecture Documentation
