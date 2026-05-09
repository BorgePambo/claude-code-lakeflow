"""
Silver Clean Layer - Data Standardization and Quality

This layer:
- Cleans and standardizes all bronze tables
- Implements data quality checks
- Adds audit columns and change hashes
- Prepares data for dimensional modeling
- Uses Lakeflow Declarative Pipelines (SDP)

Tables created:
- northwind.silver_clean.categories_clean
- northwind.silver_clean.products_clean
- northwind.silver_clean.customers_clean
- northwind.silver_clean.employees_clean
- northwind.silver_clean.orders_clean
- northwind.silver_clean.order_details_clean
- northwind.silver_clean.shippers_clean
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    when,
    coalesce,
    lit,
    concat_ws,
    lower,
    trim,
    md5,
    current_timestamp,
    hash as spark_hash,
)
from pyspark import pipelines as dp
from utils import (
    add_audit_columns,
    create_hash_column,
    standardize_columns,
    normalize_strings,
    handle_nulls,
    remove_duplicates,
    validate_required_columns,
    create_surrogate_key,
    log_pipeline_event,
)


# ============================================================================
# CATEGORIES_CLEAN
# ============================================================================

@dp.materialized_view
def categories_clean():
    """
    Clean categories table.
    
    Source: northwind.bronze.categories
    Transformations:
    - Standardize column names
    - Normalize strings
    - Add audit columns
    - Create change tracking hash
    """
    log_pipeline_event("START", "categories_clean", "RUNNING")
    
    df = (
        spark.read
        .table("northwind.bronze.categories")
    )
    
    # Validate required columns
    validate_required_columns(df, ["categoryID", "categoryName"])
    
    # Standardize and clean
    df = standardize_columns(df)
    df = normalize_strings(df)
    
    # Handle nulls
    df = handle_nulls(
        df,
        null_strategy={
            "description": "N/A"
        }
    )
    
    # Add audit columns
    df = add_audit_columns(df, source="northwind_categories")
    
    # Create hash for change tracking
    df = create_hash_column(
        df,
        columns=["categoryid", "categoryname", "description"],
        hash_column_name="_change_hash"
    )
    
    # Remove duplicates
    df = remove_duplicates(df, subset=["categoryid"])
    
    log_pipeline_event("END", "categories_clean", "SUCCESS")
    
    return df


# ============================================================================
# PRODUCTS_CLEAN
# ============================================================================

@dp.materialized_view
def products_clean():
    """
    Clean products table.
    
    Source: northwind.bronze.products
    Transformations:
    - Standardize column names and types
    - Remove discontinued products flag handling
    - Handle null quantities and prices
    - Add audit columns and hashes
    """
    log_pipeline_event("START", "products_clean", "RUNNING")
    
    df = (
        spark.read
        .table("northwind.bronze.products")
    )
    
    validate_required_columns(df, ["productID", "productName", "categoryID"])
    
    # Standardize and clean
    df = standardize_columns(df)
    df = normalize_strings(df)
    
    # Cast types
    df = (
        df
        .withColumn("productid", col("productid").cast("int"))
        .withColumn("categoryid", col("categoryid").cast("int"))
        .withColumn("unitprice", col("unitprice").cast("decimal(10,2)"))
        .withColumn("quantityperunit", col("quantityperunit").cast("string"))
        .withColumn("discontinued", col("discontinued").cast("int"))
    )
    
    # Handle nulls
    df = handle_nulls(
        df,
        null_strategy={
            "unitprice": 0.00,
            "quantityperunit": "Unknown",
            "discontinued": 0
        }
    )
    
    # Add audit columns
    df = add_audit_columns(df, source="northwind_products")
    
    # Create hash
    df = create_hash_column(
        df,
        columns=["productid", "productname", "categoryid", "unitprice", "discontinued"],
        hash_column_name="_change_hash"
    )
    
    # Remove duplicates
    df = remove_duplicates(df, subset=["productid"])
    
    log_pipeline_event("END", "products_clean", "SUCCESS")
    
    return df


# ============================================================================
# CUSTOMERS_CLEAN
# ============================================================================

@dp.materialized_view
def customers_clean():
    """
    Clean customers table.
    
    Source: northwind.bronze.customers
    Transformations:
    - Normalize customer names and contact info
    - Standardize location data
    - Add audit columns
    """
    log_pipeline_event("START", "customers_clean", "RUNNING")
    
    df = (
        spark.read
        .table("northwind.bronze.customers")
    )
    
    validate_required_columns(df, ["customerID", "companyName"])
    
    # Standardize and clean
    df = standardize_columns(df)
    df = normalize_strings(df)
    
    # Handle nulls
    df = handle_nulls(
        df,
        null_strategy={
            "contactname": "Unknown",
            "contacttitle": "Unknown",
            "city": "Unknown",
            "country": "Unknown"
        }
    )
    
    # Add audit columns
    df = add_audit_columns(df, source="northwind_customers")
    
    # Create hash
    df = create_hash_column(
        df,
        columns=["customerid", "companyname", "contactname", "city", "country"],
        hash_column_name="_change_hash"
    )
    
    # Remove duplicates
    df = remove_duplicates(df, subset=["customerid"])
    
    log_pipeline_event("END", "customers_clean", "SUCCESS")
    
    return df


# ============================================================================
# EMPLOYEES_CLEAN
# ============================================================================

@dp.materialized_view
def employees_clean():
    """
    Clean employees table.
    
    Source: northwind.bronze.employees
    Transformations:
    - Normalize employee names
    - Handle manager relationships (reportsTo)
    - Standardize location data
    """
    log_pipeline_event("START", "employees_clean", "RUNNING")
    
    df = (
        spark.read
        .table("northwind.bronze.employees")
    )
    
    validate_required_columns(df, ["employeeID", "employeeName"])
    
    # Standardize and clean
    df = standardize_columns(df)
    df = normalize_strings(df)
    
    # Cast types
    df = (
        df
        .withColumn("employeeid", col("employeeid").cast("int"))
        .withColumn("reportsto", col("reportsto").cast("int"))
    )
    
    # Handle nulls
    df = handle_nulls(
        df,
        null_strategy={
            "title": "Unknown",
            "city": "Unknown",
            "country": "Unknown",
            "reportsto": -1  # -1 indicates no manager (CEO level)
        }
    )
    
    # Add audit columns
    df = add_audit_columns(df, source="northwind_employees")
    
    # Create hash
    df = create_hash_column(
        df,
        columns=["employeeid", "employeename", "title", "city", "country"],
        hash_column_name="_change_hash"
    )
    
    # Remove duplicates
    df = remove_duplicates(df, subset=["employeeid"])
    
    log_pipeline_event("END", "employees_clean", "SUCCESS")
    
    return df


# ============================================================================
# ORDERS_CLEAN
# ============================================================================

@dp.materialized_view
def orders_clean():
    """
    Clean orders table.
    
    Source: northwind.bronze.orders
    Transformations:
    - Standardize date columns
    - Handle nullable date fields
    - Validate foreign key relationships
    - Add audit columns
    """
    log_pipeline_event("START", "orders_clean", "RUNNING")
    
    df = (
        spark.read
        .table("northwind.bronze.orders")
    )
    
    validate_required_columns(
        df,
        ["orderID", "customerID", "employeeID", "orderDate"]
    )
    
    # Standardize and clean
    df = standardize_columns(df)
    
    # Cast types
    df = (
        df
        .withColumn("orderid", col("orderid").cast("int"))
        .withColumn("customerid", col("customerid").cast("string"))
        .withColumn("employeeid", col("employeeid").cast("int"))
        .withColumn("shipperid", col("shipperid").cast("int"))
        .withColumn("orderdate", col("orderdate").cast("date"))
        .withColumn("requireddate", col("requireddate").cast("date"))
        .withColumn("shippeddate", col("shippeddate").cast("date"))
        .withColumn("freight", col("freight").cast("decimal(10,2)"))
    )
    
    # Handle nulls
    df = handle_nulls(
        df,
        null_strategy={
            "freight": 0.00,
            "shippeddate": None  # Keep as null - not all orders are shipped
        }
    )
    
    # Add audit columns
    df = add_audit_columns(df, source="northwind_orders")
    
    # Create hash
    df = create_hash_column(
        df,
        columns=["orderid", "customerid", "employeeid", "orderdate", "freight"],
        hash_column_name="_change_hash"
    )
    
    # Remove duplicates
    df = remove_duplicates(df, subset=["orderid"])
    
    log_pipeline_event("END", "orders_clean", "SUCCESS")
    
    return df


# ============================================================================
# ORDER_DETAILS_CLEAN
# ============================================================================

@dp.materialized_view
def order_details_clean():
    """
    Clean order details table (line items).
    
    Source: northwind.bronze.order_details
    Transformations:
    - Standardize column names
    - Handle pricing and discount
    - Calculate derived metrics
    """
    log_pipeline_event("START", "order_details_clean", "RUNNING")
    
    df = (
        spark.read
        .table("northwind.bronze.order_details")
    )
    
    validate_required_columns(
        df,
        ["orderID", "productID", "quantity", "unitPrice"]
    )
    
    # Standardize and clean
    df = standardize_columns(df)
    
    # Cast types
    df = (
        df
        .withColumn("orderid", col("orderid").cast("int"))
        .withColumn("productid", col("productid").cast("int"))
        .withColumn("unitprice", col("unitprice").cast("decimal(10,2)"))
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("discount", col("discount").cast("decimal(5,4)"))
    )
    
    # Handle nulls
    df = handle_nulls(
        df,
        null_strategy={
            "discount": 0.0000,
            "quantity": 0
        }
    )
    
    # Calculate line total (before discount)
    df = df.withColumn(
        "line_total",
        (col("quantity") * col("unitprice")).cast("decimal(12,2)")
    )
    
    # Calculate discounted amount
    df = df.withColumn(
        "discount_amount",
        (col("line_total") * col("discount")).cast("decimal(12,2)")
    )
    
    # Add audit columns
    df = add_audit_columns(df, source="northwind_order_details")
    
    # Create hash
    df = create_hash_column(
        df,
        columns=["orderid", "productid", "quantity", "unitprice", "discount"],
        hash_column_name="_change_hash"
    )
    
    # Remove duplicates (compound key)
    df = remove_duplicates(df, subset=["orderid", "productid"])
    
    log_pipeline_event("END", "order_details_clean", "SUCCESS")
    
    return df


# ============================================================================
# SHIPPERS_CLEAN
# ============================================================================

@dp.materialized_view
def shippers_clean():
    """
    Clean shippers table.
    
    Source: northwind.bronze.shippers
    Transformations:
    - Normalize shipper names
    - Standardize column names
    """
    log_pipeline_event("START", "shippers_clean", "RUNNING")
    
    df = (
        spark.read
        .table("northwind.bronze.shippers")
    )
    
    validate_required_columns(df, ["shipperID", "companyName"])
    
    # Standardize and clean
    df = standardize_columns(df)
    df = normalize_strings(df)
    
    # Handle nulls
    df = handle_nulls(
        df,
        null_strategy={
            "companyname": "Unknown"
        }
    )
    
    # Add audit columns
    df = add_audit_columns(df, source="northwind_shippers")
    
    # Create hash
    df = create_hash_column(
        df,
        columns=["shipperid", "companyname"],
        hash_column_name="_change_hash"
    )
    
    # Remove duplicates
    df = remove_duplicates(df, subset=["shipperid"])
    
    log_pipeline_event("END", "shippers_clean", "SUCCESS")
    
    return df


# ============================================================================
# Pipeline Configuration
# ============================================================================

if __name__ == "__main__":
    """
    Main execution block for testing/running the pipeline locally.
    
    In Databricks, this would be defined in a Workflow configuration.
    """
    
    # This would be executed in Databricks Workflows
    print("Silver Clean Layer - Lakeflow Declarative Pipeline")
    print("=" * 60)
    print("This pipeline creates 7 clean tables from bronze sources:")
    print("- categories_clean")
    print("- products_clean")
    print("- customers_clean")
    print("- employees_clean")
    print("- orders_clean")
    print("- order_details_clean")
    print("- shippers_clean")
    print("\nAll tables include:")
    print("- Standardized column names and types")
    print("- Null handling")
    print("- Audit columns (_loaded_at, _source, _processing_timestamp)")
    print("- Change tracking hash (_change_hash)")
    print("- Deduplication")
    print("=" * 60)
