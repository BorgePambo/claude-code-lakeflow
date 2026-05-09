
from pyspark import pipelines as dp
from pyspark.sql import functions as FF

# Configuration
BASE_PATH = "/Volumes/northwind/raw/datasets/northwind_traders/"
CSV_FORMAT = "csv"
ENCODING = "ISO-8859-1"
HEADER = True


base_path = "/Volumes/northwind/raw/datasets/northwind_traders/"

@dp.table(name="northwind.bronze.products")
def products():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("encoding", "ISO-8859-1")
        .load(f"{base_path}products")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


@dp.table(name="northwind.bronze.categories")
def categories():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("encoding", "ISO-8859-1")
        .load(f"{base_path}categories")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )



@dp.table(name="northwind.bronze.customers")
def customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("encoding", "ISO-8859-1")
        .load(f"{base_path}customers")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


@dp.table(name="northwind.bronze.employees")
def employees():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("encoding", "ISO-8859-1")
        .load(f"{base_path}employees")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


@dp.table(name="northwind.bronze.orders")
def orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("encoding", "ISO-8859-1")
        .load(f"{base_path}orders")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


@dp.table(name="northwind.bronze.order_details")
def order_details():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("encoding", "ISO-8859-1")
        .load(f"{base_path}order_details")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


@dp.table(name="northwind.bronze.shippers")
def shippers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("encoding", "ISO-8859-1")
        .load(f"{base_path}shippers")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )
