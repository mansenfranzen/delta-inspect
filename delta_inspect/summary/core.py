# File: delta_inspect/__init__.py

import datetime
import json
from deltalake import DeltaTable
import polars as pl
from delta_inspect.util.history import extract_operation_params
from delta_inspect.util.misc import to_datetime
from delta_inspect.util.table_loader import load_table
from delta_inspect.summary.model import ColumnStatistics, SchemaField, TableMetadata, TableStatistics, TableSummary


def _extract_schema(dt: DeltaTable) -> list[SchemaField]:
    """Extract schema information from Delta table."""
    return [SchemaField.model_validate(field) for field in dt.schema().fields]


def _extract_table_statistics(dt: DeltaTable, df_add_actions: pl.DataFrame) -> TableStatistics:
    """Extract basic file statistics from add actions."""
    if df_add_actions.is_empty():
        return TableStatistics(
            num_files=0,
            total_size_bytes=0,
            num_records=0,
            num_partitions=0
        )

    expr = [
            pl.len().alias("num_files"),
            pl.sum("size_bytes").alias("total_size_bytes"),
            pl.sum("num_records").alias("num_records")
    ]

    result = df_add_actions.select(expr).row(0, named=True)
    result["num_partitions"] = len(dt.partitions())

    return TableStatistics(**result)

def _extract_column_statistic(df_add_actions: pl.DataFrame, column: str) -> ColumnStatistics:
    if "partition_values" in df_add_actions.columns:
        partition_columns = df_add_actions["partition_values"].struct.fields
    else:
        partition_columns = []

    if column in partition_columns:
        pcol = pl.col("partition_values").struct.field(column)
        expr = [
            pcol.min().alias("min"),
            pcol.max().alias("max"),
            pl.lit(0).alias("null_count")  # Partition columns should never be null
        ]
    else:
        expr = [
            pl.col("min").struct.field(column).min().alias("min"),
            pl.col("max").struct.field(column).max().alias("max"),
            pl.col("null_count").struct.field(column).sum().alias("null_count")
        ]

    result = df_add_actions.select(expr).row(0, named=True)
    return ColumnStatistics(**result)


def _extract_column_statistics(df_add_actions: pl.DataFrame) -> dict[str, ColumnStatistics]:
    """Extract overall min/max values across all files from statistics."""
    if df_add_actions.is_empty():
        return {}
    
    columns = df_add_actions["min"].struct.fields
    return {column: _extract_column_statistic(df_add_actions, column) for column in columns}


def _extract_last_commit_timestamp(history: list[dict]) -> datetime.datetime:
    last_commit_timestamp_ms = history[0]["timestamp"]
    return datetime.datetime.fromtimestamp(last_commit_timestamp_ms / 1000)


def _extract_last_optimize_timestamp(history: list[dict]) -> datetime.datetime | None:
    for commit in history:
        if commit.get("operation") == "OPTIMIZE":
            return to_datetime(commit["timestamp"])

def _extract_last_vacuum_timestamp(history: list[dict]) -> datetime.datetime | None:
    for commit in history:
        if commit.get("operation") != "VACUUM END":
            continue

        status = commit.get('operationParameters', {}).get('status')
        if status == "COMPLETED":
            return to_datetime(commit["timestamp"])


def summary(path: str) -> TableSummary:
    """
    Summarize a Delta Lake table by analyzing its metadata and active files.
    """
    # Load the Delta table
    dt = load_table(path)
        
    version = dt.version()
    schema = _extract_schema(dt)
    meta_data = TableMetadata.model_validate(dt.metadata())
    
    # Get active files using get_add_actions (returns Arrow RecordBatch)
    add_actions_batch = dt.get_add_actions()
    df_add_actions = pl.DataFrame(add_actions_batch)
    
    table_statistics = _extract_table_statistics(dt, df_add_actions)
    column_statistics = _extract_column_statistics(df_add_actions)

    history = dt.history()
    last_commit_timestamp = _extract_last_commit_timestamp(history)
    last_vacuum_timestamp = _extract_last_vacuum_timestamp(history)
    last_optimize_timestamp = _extract_last_optimize_timestamp(history)
    clustering_columns = extract_operation_params(history=history, param_key="clusterBy")
    zorder_columns = extract_operation_params(history=history, param_key="zOrderBy")
    
    
    return TableSummary(
        version=version,
        schema=schema,
        metadata=meta_data,
        protocol=dt.protocol(),
        table_statistics=table_statistics,
        column_statistics=column_statistics,
        last_commit_timestamp=last_commit_timestamp,
        last_vacuum_timestamp=last_vacuum_timestamp,
        last_optimize_timestamp=last_optimize_timestamp,
        clustering_columns=clustering_columns,
        zorder_columns=zorder_columns
    )