from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_PATH = "/Volumes/northwind/raw/datasets/northwind_traders/"

COMMON_OPTIONS = {
    "cloudFiles.format": "csv",
    "header": "true",
    "encoding": "ISO-8859-1"
}


# ============================================================================
# PRODUCTS
# ============================================================================

@dp.table(name="products")
def products():
    return (
        spark.readStream
        .format("cloudFiles")
        .options(**COMMON_OPTIONS)
        .load(f"{BASE_PATH}products")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


# ============================================================================
# CATEGORIES
# ============================================================================

@dp.table(name="categories")
def categories():
    return (
        spark.readStream
        .format("cloudFiles")
        .options(**COMMON_OPTIONS)
        .load(f"{BASE_PATH}categories")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


# ============================================================================
# CUSTOMERS
# ============================================================================

@dp.table(name="customers")
def customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .options(**COMMON_OPTIONS)
        .load(f"{BASE_PATH}customers")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


# ============================================================================
# EMPLOYEES
# ============================================================================

@dp.table(name="employees")
def employees():
    return (
        spark.readStream
        .format("cloudFiles")
        .options(**COMMON_OPTIONS)
        .load(f"{BASE_PATH}employees")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


# ============================================================================
# ORDERS
# ============================================================================

@dp.table(name="orders")
def orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .options(**COMMON_OPTIONS)
        .load(f"{BASE_PATH}orders")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


# ============================================================================
# ORDER DETAILS
# ============================================================================

@dp.table(name="order_details")
def order_details():
    return (
        spark.readStream
        .format("cloudFiles")
        .options(**COMMON_OPTIONS)
        .load(f"{BASE_PATH}order_details")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


# ============================================================================
# SHIPPERS
# ============================================================================

@dp.table(name="shippers")
def shippers():
    return (
        spark.readStream
        .format("cloudFiles")
        .options(**COMMON_OPTIONS)
        .load(f"{BASE_PATH}shippers")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )