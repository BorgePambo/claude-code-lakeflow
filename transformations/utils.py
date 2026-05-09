"""
Utility functions for Northwind Lakeflow Declarative Pipeline.

This module provides reusable functions for:
- Data cleaning and standardization
- Audit column management
- Hash generation for change tracking
- Data quality expectations
- String normalization
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    hash,
    lower,
    md5,
    regexp_replace,
    trim,
    when,
    coalesce,
    lit,
    collect_list,
    row_number,
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime


def add_audit_columns(df: DataFrame, source: str = "unknown") -> DataFrame:
    """
    Add audit columns to track data lineage and processing time.
    
    Adds:
    - _loaded_at: Timestamp of data ingestion
    - _source: Source system identifier
    - _processing_timestamp: When the record was processed
    
    Args:
        df: Input DataFrame
        source: Source system identifier
        
    Returns:
        DataFrame with audit columns added
    """
    return (
        df
        .withColumn("_loaded_at", current_timestamp())
        .withColumn("_source", lit(source))
        .withColumn("_processing_timestamp", current_timestamp())
    )


def create_hash_column(
    df: DataFrame,
    columns: list,
    hash_column_name: str = "_change_hash"
) -> DataFrame:
    """
    Create a hash column for change data capture and deduplication.
    
    Generates MD5 hash of specified columns to track changes.
    Null values are converted to 'NULL_VALUE' string for consistency.
    
    Args:
        df: Input DataFrame
        columns: List of column names to hash
        hash_column_name: Name of the hash column to create
        
    Returns:
        DataFrame with hash column added
    """
    # Replace nulls with 'NULL_VALUE' string before hashing
    hash_columns = [
        coalesce(col(c).cast("string"), lit("NULL_VALUE"))
        for c in columns
    ]
    
    return df.withColumn(
        hash_column_name,
        md5(concat_ws("|", *hash_columns))
    )


def standardize_columns(df: DataFrame) -> DataFrame:
    """
    Standardize column names and types.
    
    Converts:
    - Column names to lowercase with underscores (snake_case)
    - Removes special characters
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with standardized column names
    """
    for col_name in df.columns:
        # Convert to lowercase and replace spaces/hyphens with underscores
        new_col_name = (
            col_name.lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace(".", "_")
        )
        if col_name != new_col_name:
            df = df.withColumnRenamed(col_name, new_col_name)
    
    return df


def normalize_strings(df: DataFrame, exclude_cols: list = None) -> DataFrame:
    """
    Normalize string columns: trim whitespace, convert to lowercase where appropriate.
    
    Args:
        df: Input DataFrame
        exclude_cols: List of column names to exclude from normalization
        
    Returns:
        DataFrame with normalized string columns
    """
    exclude_cols = exclude_cols or []
    
    for field in df.schema.fields:
        col_name = field.name
        if col_name not in exclude_cols and field.dataType == StringType():
            df = df.withColumn(col_name, trim(col(col_name)))
    
    return df


def handle_nulls(
    df: DataFrame,
    null_strategy: dict = None
) -> DataFrame:
    """
    Handle null values according to specified strategy.
    
    Args:
        df: Input DataFrame
        null_strategy: Dict mapping column names to fill values
                       Example: {"price": 0, "name": "Unknown"}
        
    Returns:
        DataFrame with nulls handled
    """
    if null_strategy is None:
        null_strategy = {}
    
    for col_name, fill_value in null_strategy.items():
        if col_name in df.columns:
            df = df.fillna({col_name: fill_value})
    
    return df


def remove_duplicates(
    df: DataFrame,
    subset: list = None,
    keep: str = "first"
) -> DataFrame:
    """
    Remove duplicate records.
    
    Args:
        df: Input DataFrame
        subset: List of column names to consider for duplicates.
                If None, considers all columns.
        keep: "first" or "last" - which duplicate to keep
        
    Returns:
        DataFrame with duplicates removed
    """
    if subset is None:
        return df.dropDuplicates()
    
    if keep == "first":
        window_spec = Window.partitionBy(*subset).orderBy()
        return df.withColumn("_rn", row_number().over(window_spec)).filter(
            col("_rn") == 1
        ).drop("_rn")
    
    return df.dropDuplicates(subset=subset)


def apply_common_expectations(df: DataFrame, table_name: str = "unknown") -> dict:
    """
    Generate common data quality expectations for expectations framework.
    
    Returns a dictionary with common expectations that can be used with
    Databricks expectations framework.
    
    Args:
        df: Input DataFrame
        table_name: Name of the table for context
        
    Returns:
        Dictionary with expectations configuration
    """
    expectations = {
        "table_name": table_name,
        "checks": [
            {
                "name": "no_nulls_in_id_columns",
                "description": "ID columns should not contain nulls",
                "rule": "column must not be null"
            },
            {
                "name": "data_freshness",
                "description": "Data should be recent",
                "rule": "_loaded_at must be within last 24 hours"
            },
            {
                "name": "row_count_check",
                "description": "Table should contain data",
                "rule": "row count must be > 0"
            },
            {
                "name": "no_duplicates",
                "description": "No duplicate primary keys",
                "rule": "no duplicates on id columns"
            }
        ]
    }
    return expectations


def create_surrogate_key(
    df: DataFrame,
    columns: list,
    output_col: str = "surrogate_key"
) -> DataFrame:
    """
    Create a surrogate key by concatenating and hashing specified columns.
    
    Args:
        df: Input DataFrame
        columns: List of column names to use for surrogate key
        output_col: Name of the output surrogate key column
        
    Returns:
        DataFrame with surrogate key column added
    """
    hash_columns = [coalesce(col(c).cast("string"), lit("NULL")) for c in columns]
    
    return df.withColumn(
        output_col,
        md5(concat_ws("|", *hash_columns))
    )


def get_incremental_config(table_name: str, checkpoint_path: str = None) -> dict:
    """
    Get configuration for incremental processing.
    
    Args:
        table_name: Name of the table being processed
        checkpoint_path: Path for checkpoint storage
        
    Returns:
        Dictionary with incremental configuration
    """
    if checkpoint_path is None:
        checkpoint_path = f"/tmp/checkpoints/{table_name}"
    
    return {
        "table_name": table_name,
        "checkpoint_path": checkpoint_path,
        "mode": "append",
        "mergeSchema": True,
        "checkpointLocation": checkpoint_path
    }


def validate_required_columns(df: DataFrame, required_cols: list) -> bool:
    """
    Validate that required columns exist in DataFrame.
    
    Args:
        df: Input DataFrame
        required_cols: List of required column names
        
    Returns:
        True if all required columns exist, raises error otherwise
    """
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        raise ValueError(
            f"Missing required columns: {missing_cols}. "
            f"Available columns: {df.columns}"
        )
    
    return True


def create_version_columns(df: DataFrame) -> DataFrame:
    """
    Add versioning columns for SCD Type 2 support.
    
    Adds:
    - _effective_date: When the record becomes effective
    - _end_date: When the record expires (null = current)
    - _is_current: Flag indicating if this is the current version
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with versioning columns
    """
    return (
        df
        .withColumn("_effective_date", current_timestamp())
        .withColumn("_end_date", lit(None).cast(TimestampType()))
        .withColumn("_is_current", lit(True))
    )


def merge_configuration(
    table_name: str,
    target_schema: str,
    join_condition: str,
    is_scd2: bool = False
) -> dict:
    """
    Generate MERGE configuration for CDC operations.
    
    Args:
        table_name: Name of the target table
        target_schema: Schema/database name
        join_condition: Condition for matching source to target
        is_scd2: Whether to apply SCD Type 2 logic
        
    Returns:
        Dictionary with MERGE configuration
    """
    return {
        "target_table": f"{target_schema}.{table_name}",
        "join_condition": join_condition,
        "is_scd2": is_scd2,
        "match_condition": join_condition,
        "update_condition": "source._change_hash != target._change_hash",
        "insert_condition": "1=1"
    }


def log_pipeline_event(event_type: str, table_name: str, status: str, message: str = "") -> None:
    """
    Log pipeline events for monitoring and debugging.
    
    Args:
        event_type: Type of event (START, END, ERROR, WARNING)
        table_name: Name of the table being processed
        status: Status of the operation (SUCCESS, FAILED, RUNNING)
        message: Additional message context
    """
    timestamp = datetime.now().isoformat()
    log_message = (
        f"[{timestamp}] [{event_type}] [{table_name}] "
        f"Status: {status} - {message}"
    )
    print(log_message)
    # In production, write to a logging table or system


# Configuration constants
AUDIT_COLUMNS = ["_loaded_at", "_source", "_processing_timestamp"]
HASH_COLUMNS_EXCLUDE = AUDIT_COLUMNS + ["_rn", "surrogate_key", "_change_hash"]
SCD2_COLUMNS = ["_effective_date", "_end_date", "_is_current"]
