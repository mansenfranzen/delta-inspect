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

console = Console()


def format_bytes(bytes_value: int) -> str:
    """Format bytes as human-readable string."""
    if bytes_value == 0:
        return "0 B"
    
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(bytes_value)
    unit_index = 0
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    
    return f"{size:.1f} {units[unit_index]}"


def format_number(num: int) -> str:
    """Format large numbers with comma separators."""
    return f"{num:,}"


class TableColumn(BaseModel):
    title: str
    style: str | None = None
    width: int = 20    

def output_table(title: str, columns: list[TableColumn], rows: list[list]):

    metadata_table = Table(title=title, show_header=True, header_style="bold magenta")
    for column in columns:
        metadata_table.add_column(column.title, style=column.style, width=column.width)
    for row in rows:
        metadata_table.add_row(*[str(item) for item in row])
    
    console.print(metadata_table)
    console.print()



def summary_command(
    path: Annotated[str, typer.Argument(help="Path to the Delta table")],
) -> None:
    """Summarize a Delta Lake table showing version, protocol, statistics, metadata and last commit timestamp."""
    
    try:
        # Get table summary
        summary = summarize_table(path)
        
        # Rich formatted output
        console.print(Panel(f"[bold blue]Delta Table Summary[/bold blue]", title="📊 delta-inspect"))
        console.print()
        
        # Basic Information
        basic_columns = [
            TableColumn(title="Property", style="cyan", width=20),
            TableColumn(title="Value", style="white", width=66)
        ]
        basic_rows = [
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
            ["Partition Columns", str(summary.metadata.partition_columns)]
        ]
        output_table("Meta Data Information", basic_columns, basic_rows)


        
        # Table Statistics
        stats_columns = [
            TableColumn(title="Metric", style="cyan", width=20),
            TableColumn(title="Value", style="white", width=66)
        ]
        stats_rows = [
            ["Number of Files", format_number(summary.table_statistics.num_files)],
            ["Number of Partitions", format_number(summary.table_statistics.num_partitions)],
            ["Number of Records", format_number(summary.table_statistics.num_records)],
            ["Total Size", format_bytes(summary.table_statistics.total_size_bytes)]
        ]
        output_table("Table Statistics", stats_columns, stats_rows)


        schema_columns = [
            TableColumn(title="Column", style="cyan"),
            TableColumn(title="Type", style="yellow"),
            TableColumn(title="Nullable", style="green", width=10),
            TableColumn(title="Metadata", style="dim", width=30)
        ]
        schema_rows = []
        for field in summary.schema_:
            metadata_str = json.dumps(field.metadata) if field.metadata else "{}"
            schema_rows.append([
                field.name,
                field.type,
                "✓" if field.nullable else "✗",
                metadata_str[:50] + "..." if len(metadata_str) > 50 else metadata_str
            ])
        output_table("Schema", schema_columns, schema_rows)
        

        col_stats_columns = [
            TableColumn(title="Column", style="cyan"),
            TableColumn(title="Min", style="green"),
            TableColumn(title="Max", style="green"),
            TableColumn(title="Null Count", style="yellow")
        ]
        col_stats_rows = []
        for col_name, col_stat in summary.column_statistics.items():
            min_val = str(col_stat.min) if col_stat.min is not None else "N/A"
            max_val = str(col_stat.max) if col_stat.max is not None else "N/A"
            
            # Truncate long values
            if len(min_val) > 30:
                min_val = min_val[:27] + "..."
            if len(max_val) > 30:
                max_val = max_val[:27] + "..."
            
            col_stats_rows.append([
                col_name,
                min_val,
                max_val,
                format_number(col_stat.null_count)
            ])
        output_table("Column Statistics", col_stats_columns, col_stats_rows)
    
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")        
        raise typer.Exit(1)

