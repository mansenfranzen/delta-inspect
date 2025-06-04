"""CLI subcommand for Delta table summary functionality."""

from pydantic import BaseModel
import typer
from typing import Annotated
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.columns import Columns
import json

from delta_inspect.summary.core import summarize_table
from delta_inspect.summary.model import TableSummary
from delta_inspect.util.cli import (
    TableColumn,
    console_header,
    format_number,
    format_bytes,
    console_table,
    WIDTH_COL_FIRST,
    WIDTH_REMAIN,
    WIDTH_TOTAL,
)

console = Console()


def create_overview_table(summary: TableSummary):
    columns = [
        TableColumn(title="Property", style="cyan", width=WIDTH_COL_FIRST),
        TableColumn(title="Value", style="white", width=WIDTH_REMAIN),
    ]

    rows = [
        ["Table ID", summary.metadata.id],
        ["Table Name", summary.metadata.name],
        ["Description", summary.metadata.description],
        ["", ""],
        ["Version", str(summary.version)],
        ["Created Time", str(summary.metadata.created_time)],
        ["Last Commit", summary.last_commit_timestamp],
        ["", ""],
        ["Reader Version", str(summary.protocol.min_reader_version)],
        ["Reader Features", str(summary.protocol.reader_features)],
        ["Writer Version", str(summary.protocol.min_writer_version)],
        ["Writer Features", str(summary.protocol.writer_features)],
        ["", ""],
        ["Partition Columns", str(summary.metadata.partition_columns)],
        ["Table Properties", json.dumps(summary.metadata.configuration, indent=2)],
    ]
    console_table(console=console, title="Overview", columns=columns, rows=rows)


def create_table_stats_table(summary: TableSummary):
    columns = [
        TableColumn(title="Metric", style="cyan", width=WIDTH_COL_FIRST),
        TableColumn(title="Value", style="white", width=WIDTH_REMAIN),
    ]

    rows = [
        ["Number of Files", format_number(summary.table_statistics.num_files)],
        [
            "Number of Partitions",
            format_number(summary.table_statistics.num_partitions),
        ],
        ["Number of Records", format_number(summary.table_statistics.num_records)],
        ["Total Size", format_bytes(summary.table_statistics.total_size_bytes)],
    ]
    console_table(console=console, title="Table Statistics", columns=columns, rows=rows)


def create_column_stats_table(summary: TableSummary):
    columns = [
        TableColumn(title="Column", style="cyan", width=WIDTH_COL_FIRST),
        TableColumn(title="Min", style="green"),
        TableColumn(title="Max", style="green"),
        TableColumn(title="Null Count", style="yellow"),
    ]

    rows = []
    for col_name, col_stat in summary.column_statistics.items():
        min_val = str(col_stat.min) if col_stat.min is not None else "N/A"
        max_val = str(col_stat.max) if col_stat.max is not None else "N/A"

        # Truncate long values
        if len(min_val) > 30:
            min_val = min_val[:27] + "..."
        if len(max_val) > 30:
            max_val = max_val[:27] + "..."

        rows.append([col_name, min_val, max_val, format_number(col_stat.null_count)])

    console_table(
        console=console, title="Column Statistics", columns=columns, rows=rows
    )


def create_schema_table(summary: TableSummary):
    columns = [
        TableColumn(title="Column", style="cyan", width=WIDTH_COL_FIRST),
        TableColumn(title="Type", style="yellow", width=30),
        TableColumn(title="Nullable", style="green", width=10),
        TableColumn(title="Metadata", style="dim", width=30),
    ]

    rows = []
    for field in summary.schema_:
        metadata_str = json.dumps(field.metadata) if field.metadata else "{}"
        metadata_str = (
            metadata_str[:50] + "..." if len(metadata_str) > 50 else metadata_str
        )
        rows.append(
            [
                field.name,
                field.type,
                "✓" if field.nullable else "✗",
                metadata_str,
            ]
        )
    console_table(console=console, title="Schema", columns=columns, rows=rows)


def summary_command(
    path: Annotated[str, typer.Argument(help="Path to the Delta table")],
) -> None:
    """Summarize a Delta Lake table showing version, protocol, statistics, metadata and last commit timestamp."""

    console_header(console=console, title="Delta Table Summary")

    summary = summarize_table(path)
    create_overview_table(summary)
    create_schema_table(summary)
    create_table_stats_table(summary)
    create_column_stats_table(summary)
