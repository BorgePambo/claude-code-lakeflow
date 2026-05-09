"""
Silver Dimensional Layer - Star Schema Modeling

This layer implements dimensional modeling with:
- Dimension tables with surrogate keys
- SCD Type 2 for dimensions (customer, product, employee)
- Fact table for sales
- Modern CDC for incremental updates
- Star schema optimized for analytics

Tables created:
DIMENSIONS:
- northwind.silver_dimensional.dim_customers
- northwind.silver_dimensional.dim_products
- northwind.silver_dimensional.dim_employees
- northwind.silver_dimensional.dim_shippers

FACTS:
- northwind.silver_dimensional.fact_sales
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    md5,
    concat_ws,
    row_number,
    coalesce,
    lit,
    current_timestamp,
    when,
    lag,
    sum as spark_sum,
    count as spark_count,
)
from pyspark import pipelines as dp
from utils import (
    add_audit_columns,
    create_hash_column,
    create_surrogate_key,
    remove_duplicates,
    validate_required_columns,
    create_version_columns,
    log_pipeline_event,
)


# ============================================================================
# DIMENSION: CUSTOMERS (SCD Type 2)
# ============================================================================

@dp.materialized_view
def dim_customers():
    """
    Customer dimension with SCD Type 2 (slowly changing dimensions).
    
    Features:
    - Surrogate key (dim_customer_sk)
    - Business key (customer_id)
    - Change tracking and versioning
    - SCD Type 2 attributes: _effective_date, _end_date, _is_current
    
    Source: northwind.silver_clean.customers_clean
    """
    log_pipeline_event("START", "dim_customers", "RUNNING")
    
    df = (
        spark.read
        .table("northwind.silver_clean.customers_clean")
    )
    
    validate_required_columns(df, ["customerid", "companyname"])
    
    # Create surrogate key
    df = create_surrogate_key(
        df,
        columns=["customerid"],
        output_col="dim_customer_sk"
    )
    
    # Rename business key
    df = df.withColumnRenamed("customerid", "customer_id")
    
    # Select and reorder columns
    df = df.select(
        "dim_customer_sk",
        "customer_id",
        "companyname",
        "contactname",
        "contacttitle",
        "city",
        "country",
        "_loaded_at",
        "_source",
        "_processing_timestamp",
        "_change_hash",
        "_loaded_at"
    )
    
    # Add SCD Type 2 columns
    df = create_version_columns(df)
    
    log_pipeline_event("END", "dim_customers", "SUCCESS")
    
    return df


# ============================================================================
# DIMENSION: PRODUCTS (SCD Type 2)
# ============================================================================

@dp.materialized_view
def dim_products():
    """
    Product dimension with SCD Type 2.
    
    Joins:
    - products_clean + categories_clean
    
    Features:
    - Surrogate key (dim_product_sk)
    - Business key (product_id)
    - Category enrichment
    - Version tracking for SCD Type 2
    
    Source: northwind.silver_clean.products_clean + categories_clean
    """
    log_pipeline_event("START", "dim_products", "RUNNING")
    
    products = (
        spark.read
        .table("northwind.silver_clean.products_clean")
    )
    
    categories = (
        spark.read
        .table("northwind.silver_clean.categories_clean")
    )
    
    validate_required_columns(products, ["productid"])
    validate_required_columns(categories, ["categoryid"])
    
    # Join with categories
    df = (
        products
        .join(
            categories,
            (products.categoryid == categories.categoryid),
            "left"
        )
    )
    
    # Create surrogate key
    df = create_surrogate_key(
        df,
        columns=["productid"],
        output_col="dim_product_sk"
    )
    
    # Rename columns
    df = (
        df
        .withColumnRenamed("productid", "product_id")
        .withColumnRenamed("productname", "product_name")
        .withColumnRenamed("categoryid", "category_id")
        .withColumnRenamed("categoryname", "category_name")
        .withColumnRenamed("quantityperunit", "quantity_per_unit")
        .withColumnRenamed("unitprice", "unit_price")
    )
    
    # Select and reorder columns
    df = df.select(
        "dim_product_sk",
        "product_id",
        "product_name",
        "category_id",
        "category_name",
        "unit_price",
        "quantity_per_unit",
        "discontinued",
        col("products._loaded_at").alias("_loaded_at"),
        col("products._source").alias("_source"),
        col("products._processing_timestamp").alias("_processing_timestamp"),
        col("products._change_hash").alias("_change_hash")
    )
    
    # Add SCD Type 2 columns
    df = create_version_columns(df)
    
    log_pipeline_event("END", "dim_products", "SUCCESS")
    
    return df


# ============================================================================
# DIMENSION: EMPLOYEES (SCD Type 2)
# ============================================================================

@dp.materialized_view
def dim_employees():
    """
    Employee dimension with SCD Type 2.
    
    Features:
    - Surrogate key (dim_employee_sk)
    - Business key (employee_id)
    - Manager self-join for hierarchy
    - Version tracking
    
    Source: northwind.silver_clean.employees_clean
    """
    log_pipeline_event("START", "dim_employees", "RUNNING")
    
    employees = (
        spark.read
        .table("northwind.silver_clean.employees_clean")
    )
    
    validate_required_columns(employees, ["employeeid"])
    
    # Create surrogate key
    df = create_surrogate_key(
        employees,
        columns=["employeeid"],
        output_col="dim_employee_sk"
    )
    
    # Rename columns
    df = (
        df
        .withColumnRenamed("employeeid", "employee_id")
        .withColumnRenamed("employeename", "employee_name")
        .withColumnRenamed("reportsto", "reports_to_id")
    )
    
    # Join with itself for manager info (left to handle CEOs)
    df_managers = employees.select(
        col("employeeid").alias("reports_to_id"),
        col("employeename").alias("manager_name")
    )
    
    df = df.join(df_managers, on="reports_to_id", how="left")
    
    # Select and reorder columns
    df = df.select(
        "dim_employee_sk",
        "employee_id",
        "employee_name",
        "title",
        "city",
        "country",
        "reports_to_id",
        "manager_name",
        "_loaded_at",
        "_source",
        "_processing_timestamp",
        "_change_hash"
    )
    
    # Add SCD Type 2 columns
    df = create_version_columns(df)
    
    log_pipeline_event("END", "dim_employees", "SUCCESS")
    
    return df


# ============================================================================
# DIMENSION: SHIPPERS
# ============================================================================

@dp.materialized_view
def dim_shippers():
    """
    Shipper (carrier) dimension.
    
    A simple, slowly-changing dimension.
    
    Source: northwind.silver_clean.shippers_clean
    """
    log_pipeline_event("START", "dim_shippers", "RUNNING")
    
    df = (
        spark.read
        .table("northwind.silver_clean.shippers_clean")
    )
    
    validate_required_columns(df, ["shipperid", "companyname"])
    
    # Create surrogate key
    df = create_surrogate_key(
        df,
        columns=["shipperid"],
        output_col="dim_shipper_sk"
    )
    
    # Rename columns
    df = (
        df
        .withColumnRenamed("shipperid", "shipper_id")
        .withColumnRenamed("companyname", "shipper_name")
    )
    
    # Select columns
    df = df.select(
        "dim_shipper_sk",
        "shipper_id",
        "shipper_name",
        "_loaded_at",
        "_source",
        "_processing_timestamp",
        "_change_hash"
    )
    
    log_pipeline_event("END", "dim_shippers", "SUCCESS")
    
    return df


# ============================================================================
# DIMENSION: DATE (Time Dimension)
# ============================================================================

@dp.materialized_view
def dim_dates():
    """
    Date dimension for temporal analysis.
    
    Provides:
    - date_key (YYYYMMDD format)
    - Full date
    - Year, quarter, month, day of week
    - Is weekend flag
    - Is holiday flag
    
    Note: In a real scenario, this would be generated separately.
    For now, we extract unique dates from orders.
    """
    log_pipeline_event("START", "dim_dates", "RUNNING")
    
    from pyspark.sql.functions import date_format, year, quarter, month, dayofweek
    
    # Extract distinct dates from orders
    order_dates = (
        spark.read
        .table("northwind.silver_clean.orders_clean")
        .select(col("orderdate"))
        .distinct()
        .filter(col("orderdate").isNotNull())
    )
    
    df = (
        order_dates
        .withColumn(
            "date_key",
            date_format(col("orderdate"), "yyyyMMdd").cast("int")
        )
        .withColumn("full_date", col("orderdate"))
        .withColumn("year", year(col("orderdate")))
        .withColumn("quarter", quarter(col("orderdate")))
        .withColumn("month", month(col("orderdate")))
        .withColumn("day_of_week", dayofweek(col("orderdate")))
        .withColumn(
            "is_weekend",
            when(col("day_of_week").isin([1, 7]), 1).otherwise(0)
        )
        .withColumn("is_holiday", lit(0))  # Would be updated with actual holidays
    )
    
    df = df.select(
        "date_key",
        "full_date",
        "year",
        "quarter",
        "month",
        "day_of_week",
        "is_weekend",
        "is_holiday"
    )
    
    log_pipeline_event("END", "dim_dates", "SUCCESS")
    
    return df


# ============================================================================
# FACT TABLE: SALES
# ============================================================================

@dp.materialized_view
def fact_sales():
    """
    Sales fact table with CDC support.
    
    Joins:
    - orders_clean + order_details_clean (fact join)
    - customers_clean, employees_clean, products_clean, shippers_clean (dimensions)
    
    Grain:
    - One row per order-product combination
    
    Measures:
    - quantity
    - unit_price
    - discount
    - line_total
    - discount_amount
    
    Features:
    - Foreign keys to all dimensions
    - CDC-ready structure for incremental updates
    - Fact-level audit columns
    
    Source: northwind.silver_clean.orders_clean + order_details_clean
    """
    log_pipeline_event("START", "fact_sales", "RUNNING")
    
    # Load clean tables
    orders = spark.read.table("northwind.silver_clean.orders_clean")
    order_details = spark.read.table("northwind.silver_clean.order_details_clean")
    customers = spark.read.table("northwind.silver_clean.customers_clean")
    employees = spark.read.table("northwind.silver_clean.employees_clean")
    products = spark.read.table("northwind.silver_clean.products_clean")
    shippers = spark.read.table("northwind.silver_clean.shippers_clean")
    
    # Validate required columns
    validate_required_columns(orders, ["orderid"])
    validate_required_columns(order_details, ["orderid", "productid"])
    
    # Start with order details (grain: order + product)
    df = order_details.join(orders, on="orderid", how="inner")
    
    # Join with dimensions to get surrogate keys
    # Customer dimension
    df = df.join(
        customers.select(
            col("customerid").alias("cust_id"),
            col("_change_hash").alias("cust_hash")
        ),
        col("customerid") == col("cust_id"),
        how="left"
    ).drop("cust_id")
    
    # Employee dimension
    df = df.join(
        employees.select(
            col("employeeid").alias("emp_id"),
            col("_change_hash").alias("emp_hash")
        ),
        col("employeeid") == col("emp_id"),
        how="left"
    ).drop("emp_id")
    
    # Product dimension
    df = df.join(
        products.select(
            col("productid").alias("prod_id"),
            col("_change_hash").alias("prod_hash")
        ),
        col("productid") == col("prod_id"),
        how="left"
    ).drop("prod_id")
    
    # Shipper dimension
    df = df.join(
        shippers.select(
            col("shipperid").alias("ship_id"),
            col("_change_hash").alias("ship_hash")
        ),
        col("shipperid") == col("ship_id"),
        how="left"
    ).drop("ship_id")
    
    # Create fact-level surrogate key
    df = create_surrogate_key(
        df,
        columns=["orderid", "productid"],
        output_col="fact_sales_sk"
    )
    
    # Rename foreign keys
    df = (
        df
        .withColumnRenamed("customerid", "customer_id")
        .withColumnRenamed("employeeid", "employee_id")
        .withColumnRenamed("productid", "product_id")
        .withColumnRenamed("shipperid", "shipper_id")
    )
    
    # Select final columns in logical order
    df = df.select(
        # Surrogate and business keys
        "fact_sales_sk",
        "orderid",
        col("customer_id").alias("customer_id"),
        col("employee_id").alias("employee_id"),
        col("product_id").alias("product_id"),
        col("shipper_id").alias("shipper_id"),
        # Measures from order_details
        col("quantity").alias("order_quantity"),
        col("unitprice").alias("unit_price"),
        col("discount").alias("discount_rate"),
        col("line_total").alias("line_total"),
        col("discount_amount").alias("discount_amount"),
        # Order dates
        col("orderdate").alias("order_date"),
        col("requireddate").alias("required_date"),
        col("shippeddate").alias("shipped_date"),
        col("freight").alias("freight"),
        # Audit columns
        col("_loaded_at").alias("_loaded_at"),
        col("_source").alias("_source"),
        col("_processing_timestamp").alias("_processing_timestamp"),
        col("_change_hash").alias("_change_hash")
    )
    
    # Add CDC marker for incremental processing
    df = df.withColumn("_is_deleted", lit(False))
    
    log_pipeline_event("END", "fact_sales", "SUCCESS")
    
    return df


# ============================================================================
# Pipeline Configuration and Execution
# ============================================================================

if __name__ == "__main__":
    """
    Dimensional layer pipeline configuration.
    
    This creates the star schema with:
    - 4 dimensions (customers, products, employees, shippers)
    - 1 date dimension
    - 1 fact table (sales)
    """
    
    print("Silver Dimensional Layer - Star Schema")
    print("=" * 60)
    print("\nDIMENSIONS (with SCD Type 2):")
    print("- dim_customers: Customer master data")
    print("- dim_products: Product + Category master")
    print("- dim_employees: Employee + Manager hierarchy")
    print("- dim_shippers: Shipping carrier master")
    print("- dim_dates: Temporal dimension")
    print("\nFACT TABLES:")
    print("- fact_sales: Sales transactions (grain: order × product)")
    print("\nStar Schema Structure:")
    print("         dim_customers")
    print("              |")
    print("    dim_employees --- fact_sales --- dim_products")
    print("              |              |")
    print("         dim_shippers    dim_dates")
    print("=" * 60)
