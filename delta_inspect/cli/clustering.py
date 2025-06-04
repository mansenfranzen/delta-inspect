"""CLI subcommand for Delta table clustering health functionality."""

import typer
from typing import Annotated
from rich.console import Console

from delta_inspect.clustering.core import clustering_health
from delta_inspect.clustering.model import ClusteringHealth
from delta_inspect.util.cli import (
    WIDTH_COL_FIRST,
    TableColumn,
    console_header,
    format_number,
    console_table,
)

console = Console()


def format_overlap_description(health: ClusteringHealth) -> str:
    """Create a human-readable description of overlap statistics."""
    if health.max == 0:
        return "✅   Perfect clustering"
    elif health.q50 < 0.5:
        return "✅   Good clustering"
    elif health.q50 < 2.0:
        return "⚠️   Moderate clustering"
    else:
        return "❌   Poor clustering"


def create_overview_table(health: ClusteringHealth):
    columns = [
        TableColumn(title="Metric", style="cyan", width=WIDTH_COL_FIRST),
        TableColumn(title="Value", style="white", width=30),
    ]

    rows = [
        ["Columns Analyzed", str(health.columns)],
        ["Columns Partitioned", str(health.dt.metadata().partition_columns)],
        ["", ""],
        ["File Count - Total", format_number(health.count_files_total)],
        ["File Count - No Overlap", format_number(health.count_files_no_overlap)],
        ["File Count - With Overlap", format_number(health.count_files_with_overlap)],
        ["", ""],
        ["Minimum", format_number(health.min)],
        ["5th Percentile", format_number(health.q05)],
        ["25th Percentile", format_number(health.q25)],
        ["Median (50th)", format_number(health.q50)],
        ["75th Percentile", format_number(health.q75)],
        ["95th Percentile", format_number(health.q95)],
        ["Maximum", format_number(health.max)],
        ["Mean", format_number(health.mean)],
        ["Std Deviation", format_number(health.std)],
        ["", ""],
        ["Assessment", format_overlap_description(health)],
    ]

    console_table(
        console=console, title="Overlap Statistics", columns=columns, rows=rows
    )


def create_histogram_table(health: ClusteringHealth):
    """Create a Rich table for the histogram of overlap counts."""

    columns = [
        TableColumn(title="Overlap Range", style="cyan", width=15),
        TableColumn(title="Count", style="white", width=10),
        TableColumn(title="Percentage", style="white", width=12),
        TableColumn(title="Bar", style="green"),
    ]

    count_total = sum(health.hist_cnts)
    count_max = max(health.hist_cnts)
    bin_count = len(health.hist_bins)

    rows = []
    for idx, (bin_current, count) in enumerate(zip(health.hist_bins, health.hist_cnts)):
        if idx == bin_count - 1:
            range_str = f"[{bin_current}-∞"
        else:
            bin_next = health.hist_bins[idx + 1]
            range_str = f"[{bin_current}-{bin_next})"

        percentage = (count / count_total * 100) if count_total > 0 else 0
        bar_length = int((count / count_max) * 20) if count_max > 0 else 0
        bar = "█" * bar_length

        rows.append([range_str, format_number(count), f"{percentage:.1f}%", bar])

    console_table(console=console, title="Histogram", columns=columns, rows=rows)


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
    create_overview_table(health)
    create_histogram_table(health)
