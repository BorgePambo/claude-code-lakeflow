"""
Gold Layer - Analytics and BI Aggregations

This layer creates optimized, aggregated tables for consumption by:
- Power BI
- Databricks SQL
- Data Science and Analytics teams

Tables created:
- gold_sales_by_country: Sales aggregated by country
- gold_sales_by_category: Sales aggregated by product category
- gold_sales_by_employee: Sales performance by employee
- gold_top_products: Top selling products by various metrics
- gold_kpis: Key performance indicators and business metrics
- gold_monthly_trends: Monthly sales trends
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    count as spark_count,
    avg as spark_avg,
    min as spark_min,
    max as spark_max,
    round,
    coalesce,
    lit,
    current_timestamp,
    year,
    month,
    quarter,
    date_trunc,
    rank,
    row_number,
    dense_rank,
)
from pyspark.sql import Window
from pyspark import pipelines as dp
from utils import add_audit_columns, log_pipeline_event


# ============================================================================
# GOLD: SALES BY COUNTRY
# ============================================================================

@dp.materialized_view
def gold_sales_by_country():
    """
    Sales aggregated by country.
    
    Provides:
    - Total sales amount
    - Total quantity sold
    - Number of orders and customers
    - Average order value
    - Region performance comparison
    
    Source: northwind.silver_dimensional.fact_sales + dimensions
    """
    log_pipeline_event("START", "gold_sales_by_country", "RUNNING")
    
    # Load fact and customer dimension
    fact_sales = spark.read.table("northwind.silver_dimensional.fact_sales")
    dim_customers = spark.read.table("northwind.silver_dimensional.dim_customers")
    
    # Join to get customer location
    df = (
        fact_sales
        .join(
            dim_customers.select(
                col("customer_id"),
                col("country")
            ),
            on="customer_id",
            how="inner"
        )
    )
    
    # Aggregate by country
    df = (
        df
        .groupBy("country")
        .agg(
            spark_sum("line_total").alias("total_sales"),
            spark_sum("order_quantity").alias("total_quantity"),
            spark_count("orderid").alias("total_orders"),
            spark_count(col("customer_id")).alias("unique_customers"),
            spark_avg("line_total").alias("avg_order_value"),
            spark_min("line_total").alias("min_order_value"),
            spark_max("line_total").alias("max_order_value"),
            spark_count(col("discount_rate").filter(col("discount_rate") > 0))
            .alias("orders_with_discount")
        )
    )
    
    # Calculate metrics
    df = (
        df
        .withColumn("discount_percentage",
            round(
                (col("orders_with_discount") / col("total_orders")) * 100,
                2
            )
        )
        .withColumn("avg_order_value", round(col("avg_order_value"), 2))
        .withColumn("total_sales", round(col("total_sales"), 2))
    )
    
    # Add audit columns
    df = add_audit_columns(df, source="gold_sales_by_country")
    
    log_pipeline_event("END", "gold_sales_by_country", "SUCCESS")
    
    return df


# ============================================================================
# GOLD: SALES BY CATEGORY
# ============================================================================

@dp.materialized_view
def gold_sales_by_category():
    """
    Product sales aggregated by category.
    
    Provides:
    - Sales amount and quantity by category
    - Product count per category
    - Top and bottom performers
    - Category revenue contribution
    
    Source: northwind.silver_dimensional.fact_sales + dim_products
    """
    log_pipeline_event("START", "gold_sales_by_category", "RUNNING")
    
    fact_sales = spark.read.table("northwind.silver_dimensional.fact_sales")
    dim_products = spark.read.table("northwind.silver_dimensional.dim_products")
    
    # Join to get category information
    df = (
        fact_sales
        .join(
            dim_products.select(
                col("product_id"),
                col("category_name"),
                col("discontinued")
            ),
            on="product_id",
            how="inner"
        )
    )
    
    # Aggregate by category
    df = (
        df
        .groupBy("category_name")
        .agg(
            spark_sum("line_total").alias("total_sales"),
            spark_sum("order_quantity").alias("total_quantity"),
            spark_count("orderid").alias("total_orders"),
            spark_count(col("product_id").filter(col("discontinued") == 0))
            .alias("active_products"),
            spark_count(col("product_id").filter(col("discontinued") == 1))
            .alias("discontinued_products"),
            spark_avg("unit_price").alias("avg_unit_price"),
            spark_avg("discount_rate").alias("avg_discount_rate")
        )
    )
    
    # Calculate additional metrics
    total_sales_window = Window.partitionBy()
    df = (
        df
        .withColumn("total_all_sales", spark_sum("total_sales").over(total_sales_window))
        .withColumn(
            "revenue_contribution",
            round((col("total_sales") / col("total_all_sales")) * 100, 2)
        )
        .drop("total_all_sales")
        .withColumn("total_sales", round(col("total_sales"), 2))
        .withColumn("avg_unit_price", round(col("avg_unit_price"), 2))
        .withColumn("avg_discount_rate", round(col("avg_discount_rate"), 4))
    )
    
    # Add audit columns
    df = add_audit_columns(df, source="gold_sales_by_category")
    
    log_pipeline_event("END", "gold_sales_by_category", "SUCCESS")
    
    return df


# ============================================================================
# GOLD: SALES BY EMPLOYEE
# ============================================================================

@dp.materialized_view
def gold_sales_by_employee():
    """
    Employee sales performance and ranking.
    
    Provides:
    - Total sales and order count per employee
    - Sales ranking
    - Territory performance
    - Team summary (manager view)
    
    Source: northwind.silver_dimensional.fact_sales + dim_employees
    """
    log_pipeline_event("START", "gold_sales_by_employee", "RUNNING")
    
    fact_sales = spark.read.table("northwind.silver_dimensional.fact_sales")
    dim_employees = spark.read.table("northwind.silver_dimensional.dim_employees")
    
    # Join to get employee information
    df = (
        fact_sales
        .join(
            dim_employees.select(
                col("employee_id"),
                col("employee_name"),
                col("title"),
                col("city"),
                col("country"),
                col("manager_name")
            ),
            on="employee_id",
            how="inner"
        )
    )
    
    # Aggregate by employee
    df = (
        df
        .groupBy(
            "employee_id",
            "employee_name",
            "title",
            "city",
            "country",
            "manager_name"
        )
        .agg(
            spark_sum("line_total").alias("total_sales"),
            spark_sum("order_quantity").alias("total_quantity"),
            spark_count("orderid").alias("total_orders"),
            spark_count(col("customer_id")).alias("unique_customers"),
            spark_avg("line_total").alias("avg_order_value"),
            spark_avg("discount_rate").alias("avg_discount_rate"),
            spark_min("order_date").alias("first_order_date"),
            spark_max("order_date").alias("last_order_date")
        )
    )
    
    # Add ranking
    window_rank = Window.orderBy(col("total_sales").desc())
    df = (
        df
        .withColumn("sales_rank", rank().over(window_rank))
        .withColumn("sales_percentile", 
            round((col("sales_rank") - 1) / spark_count("*").over(Window.partitionBy()) * 100, 2)
        )
    )
    
    # Calculate metrics
    df = (
        df
        .withColumn("total_sales", round(col("total_sales"), 2))
        .withColumn("avg_order_value", round(col("avg_order_value"), 2))
        .withColumn("avg_discount_rate", round(col("avg_discount_rate"), 4))
    )
    
    # Add audit columns
    df = add_audit_columns(df, source="gold_sales_by_employee")
    
    log_pipeline_event("END", "gold_sales_by_employee", "SUCCESS")
    
    return df


# ============================================================================
# GOLD: TOP PRODUCTS
# ============================================================================

@dp.materialized_view
def gold_top_products():
    """
    Top-performing products ranked by various metrics.
    
    Provides:
    - Product sales ranking
    - Quantity sold ranking
    - Customer preference ranking
    - Product profitability
    
    Source: northwind.silver_dimensional.fact_sales + dim_products
    """
    log_pipeline_event("START", "gold_top_products", "RUNNING")
    
    fact_sales = spark.read.table("northwind.silver_dimensional.fact_sales")
    dim_products = spark.read.table("northwind.silver_dimensional.dim_products")
    
    # Join to get product information
    df = (
        fact_sales
        .join(
            dim_products.select(
                col("product_id"),
                col("product_name"),
                col("category_name"),
                col("unit_price"),
                col("discontinued")
            ),
            on="product_id",
            how="inner"
        )
    )
    
    # Aggregate by product
    df = (
        df
        .groupBy(
            "product_id",
            "product_name",
            "category_name",
            "unit_price",
            "discontinued"
        )
        .agg(
            spark_sum("line_total").alias("total_sales"),
            spark_sum("order_quantity").alias("total_quantity"),
            spark_count("orderid").alias("order_count"),
            spark_count(col("customer_id")).alias("unique_customers"),
            spark_avg("discount_rate").alias("avg_discount_rate"),
            spark_avg("line_total").alias("avg_order_value")
        )
    )
    
    # Add multiple ranking dimensions
    window_by_sales = Window.orderBy(col("total_sales").desc())
    window_by_quantity = Window.orderBy(col("total_quantity").desc())
    window_by_customers = Window.orderBy(col("unique_customers").desc())
    
    df = (
        df
        .withColumn("rank_by_sales", rank().over(window_by_sales))
        .withColumn("rank_by_quantity", rank().over(window_by_quantity))
        .withColumn("rank_by_popularity", rank().over(window_by_customers))
    )
    
    # Add status
    df = df.withColumn(
        "product_status",
        when(col("discontinued") == 1, "Discontinued")
        .when(col("order_count") == 0, "No Sales")
        .otherwise("Active")
    )
    
    # Calculate metrics
    df = (
        df
        .withColumn("total_sales", round(col("total_sales"), 2))
        .withColumn("avg_order_value", round(col("avg_order_value"), 2))
        .withColumn("avg_discount_rate", round(col("avg_discount_rate"), 4))
    )
    
    # Filter to top 50 by sales
    df = df.filter(col("rank_by_sales") <= 50)
    
    # Add audit columns
    df = add_audit_columns(df, source="gold_top_products")
    
    log_pipeline_event("END", "gold_top_products", "SUCCESS")
    
    return df


# ============================================================================
# GOLD: KPIs (Key Performance Indicators)
# ============================================================================

@dp.materialized_view
def gold_kpis():
    """
    Executive-level KPIs and business metrics.
    
    Provides:
    - Total sales revenue
    - Total orders placed
    - Average transaction value
    - Customer count
    - Product count
    - Order frequency and patterns
    - Discount impact
    
    Source: northwind.silver_dimensional.fact_sales + dimensions
    """
    log_pipeline_event("START", "gold_kpis", "RUNNING")
    
    fact_sales = spark.read.table("northwind.silver_dimensional.fact_sales")
    dim_customers = spark.read.table("northwind.silver_dimensional.dim_customers")
    dim_products = spark.read.table("northwind.silver_dimensional.dim_products")
    dim_employees = spark.read.table("northwind.silver_dimensional.dim_employees")
    
    # Calculate KPIs
    kpis = fact_sales.agg(
        spark_sum("line_total").alias("total_sales"),
        spark_count("orderid").alias("total_orders"),
        spark_count(col("customer_id")).alias("unique_customers"),
        spark_count(col("product_id")).alias("unique_products_sold"),
        spark_avg("line_total").alias("avg_order_value"),
        spark_sum("order_quantity").alias("total_units_sold"),
        spark_sum(
            when(col("discount_rate") > 0, col("discount_amount"))
            .otherwise(0)
        ).alias("total_discount_amount"),
        spark_avg("discount_rate").alias("avg_discount_rate"),
        spark_min("order_date").alias("first_order_date"),
        spark_max("order_date").alias("last_order_date")
    )
    
    # Get distinct counts from dimensions
    customer_count = dim_customers.select(col("customer_id")).distinct().count()
    product_count = dim_products.select(col("product_id")).distinct().count()
    employee_count = dim_employees.select(col("employee_id")).distinct().count()
    
    # Convert to DataFrame for output
    df = kpis.select(
        col("total_sales").cast("double").alias("total_sales"),
        col("total_orders").cast("bigint").alias("total_orders"),
        col("unique_customers").cast("bigint").alias("unique_customers"),
        col("unique_products_sold").cast("bigint").alias("unique_products_sold"),
        col("avg_order_value").cast("double").alias("avg_order_value"),
        col("total_units_sold").cast("bigint").alias("total_units_sold"),
        col("total_discount_amount").cast("double").alias("total_discount_amount"),
        col("avg_discount_rate").cast("double").alias("avg_discount_rate"),
        lit(customer_count).alias("total_customers_in_database"),
        lit(product_count).alias("total_products_in_database"),
        lit(employee_count).alias("total_employees"),
        col("first_order_date").alias("first_order_date"),
        col("last_order_date").alias("last_order_date")
    )
    
    # Calculate derived metrics
    df = (
        df
        .withColumn(
            "customer_conversion_rate",
            round((col("unique_customers") / col("total_customers_in_database")) * 100, 2)
        )
        .withColumn(
            "discount_impact",
            round((col("total_discount_amount") / col("total_sales")) * 100, 2)
        )
        .withColumn(
            "avg_order_size",
            round(col("total_units_sold") / col("total_orders"), 2)
        )
        .withColumn("total_sales", round(col("total_sales"), 2))
        .withColumn("avg_order_value", round(col("avg_order_value"), 2))
        .withColumn("total_discount_amount", round(col("total_discount_amount"), 2))
        .withColumn("avg_discount_rate", round(col("avg_discount_rate"), 4))
    )
    
    # Add metadata
    df = (
        df
        .withColumn("kpi_type", lit("executive_summary"))
        .withColumn("report_date", current_timestamp())
    )
    
    # Add audit columns
    df = add_audit_columns(df, source="gold_kpis")
    
    log_pipeline_event("END", "gold_kpis", "SUCCESS")
    
    return df


# ============================================================================
# GOLD: MONTHLY TRENDS
# ============================================================================

@dp.materialized_view
def gold_monthly_trends():
    """
    Monthly sales trends for time-series analysis.
    
    Provides:
    - Monthly sales aggregates
    - Trend analysis
    - Month-over-month comparison
    - Year-over-year comparison
    
    Source: northwind.silver_dimensional.fact_sales
    """
    log_pipeline_event("START", "gold_monthly_trends", "RUNNING")
    
    fact_sales = spark.read.table("northwind.silver_dimensional.fact_sales")
    
    # Extract date components
    df = (
        fact_sales
        .withColumn("year", year(col("order_date")))
        .withColumn("month", month(col("order_date")))
        .withColumn("year_month", date_trunc("month", col("order_date")))
    )
    
    # Aggregate by year-month
    df = (
        df
        .groupBy("year_month", "year", "month")
        .agg(
            spark_sum("line_total").alias("monthly_sales"),
            spark_sum("order_quantity").alias("monthly_quantity"),
            spark_count("orderid").alias("monthly_orders"),
            spark_count(col("customer_id")).alias("monthly_customers"),
            spark_avg("line_total").alias("avg_order_value"),
            spark_avg("discount_rate").alias("avg_discount_rate")
        )
    )
    
    # Add moving averages (previous 3 months)
    window_ma = Window.orderBy("year_month").rangeBetween(-3 * 30 * 24 * 60 * 60, 0)
    df = (
        df
        .withColumn(
            "sales_3m_moving_avg",
            round(spark_avg("monthly_sales").over(window_ma), 2)
        )
    )
    
    # Calculate month-over-month change
    window_lag = Window.orderBy("year_month")
    df = (
        df
        .withColumn(
            "prev_month_sales",
            lag("monthly_sales").over(window_lag)
        )
        .withColumn(
            "mom_sales_change",
            round(
                ((col("monthly_sales") - col("prev_month_sales")) / col("prev_month_sales")) * 100,
                2
            )
        )
        .drop("prev_month_sales")
    )
    
    # Format metrics
    df = (
        df
        .withColumn("monthly_sales", round(col("monthly_sales"), 2))
        .withColumn("avg_order_value", round(col("avg_order_value"), 2))
        .withColumn("avg_discount_rate", round(col("avg_discount_rate"), 4))
    )
    
    # Add audit columns
    df = add_audit_columns(df, source="gold_monthly_trends")
    
    log_pipeline_event("END", "gold_monthly_trends", "SUCCESS")
    
    return df


# ============================================================================
# Pipeline Configuration
# ============================================================================

if __name__ == "__main__":
    """
    Gold layer pipeline configuration.
    
    This creates optimized analytics tables for BI consumption.
    """
    
    print("Gold Layer - Analytics and BI Aggregations")
    print("=" * 60)
    print("\nTables created:")
    print("- gold_sales_by_country: Sales analysis by geography")
    print("- gold_sales_by_category: Product category performance")
    print("- gold_sales_by_employee: Employee sales rankings")
    print("- gold_top_products: Top 50 products by various metrics")
    print("- gold_kpis: Executive KPIs and business metrics")
    print("- gold_monthly_trends: Time-series sales trends")
    print("\nOptimized for:")
    print("- Power BI dashboards")
    print("- Databricks SQL queries")
    print("- Data science analysis")
    print("=" * 60)
