"""CLI subcommand for Delta table clustering health functionality."""

import typer
from typing import Annotated
from rich.console import Console

from delta_inspect.clustering.core import clustering_health
from delta_inspect.clustering.model import Clustering
from delta_inspect.util.cli import (
    WIDTH_COL_FIRST,
    TableColumn,
    console_dist_histogram,
    console_dist_statistics,
    console_header,
    format_number,
    console_table,
)

console = Console()


def format_overlap_description(health: Clustering) -> str:
    """Create a human-readable description of overlap statistics."""
    if health.max == 0:
        return "✅   Perfect clustering"
    elif health.q50 < 0.5:
        return "✅   Good clustering"
    elif health.q50 < 2.0:
        return "⚠️   Moderate clustering"
    else:
        return "❌   Poor clustering"


def console_overview(health: Clustering):
    columns = [
        TableColumn(title="Metric", style="cyan", width=WIDTH_COL_FIRST),
        TableColumn(title="Value", style="white", width=30),
    ]

    rows = [
        ["Columns Analyzed", str(health.analyzed_columns)],
        ["Columns Partitioned", str(health.partition_columns)],
        ["Columns Clustering", str(health.clustering_columns)],
        ["Columns ZOrder", str(health.zorder_columns)],
        ["", ""],
        ["File Count - Total", format_number(health.count)],
        ["File Count - No Overlap", format_number(health.count_no_overlap)],
        ["File Count - With Overlap", format_number(health.count_with_overlap)],
        ["File Count - No Min/Max", format_number(health.count_without_min_max)],
        ["", ""],
        ["Assessment", format_overlap_description(health)],
    ]

    console_table(
        console=console, title="Overlap Statistics", columns=columns, rows=rows
    )


def clustering_command(
    path: Annotated[str, typer.Argument(help="Path to the Delta table")],
    columns: Annotated[
        list[str],
        typer.Option(
            "--columns",
            "-c",
            help="Columns to analyze for clustering (can be specified multiple times)",
        ),
    ],
) -> None:
    """Analyze clustering health of a Delta Lake table for specified columns."""

    if not columns:
        err_msg = "At least one column must be specified using --columns/-c"
        console.print(err_msg)
        raise typer.Exit(1)

    console_header(console=console, title="Clustering Health")

    health = clustering_health(path, columns)
    console_overview(health)
    console_dist_statistics(dist=health, metric="Overlaps", console=console)
    console_dist_histogram(dist=health, metric="Overlaps", console=console)
