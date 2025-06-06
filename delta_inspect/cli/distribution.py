"""CLI subcommand for Delta table clustering health functionality."""

import typer
from typing import Annotated
from rich.console import Console

from delta_inspect.distribution.core import distribution
from delta_inspect.distribution.model import Distribution, DistributionMetric, ItemDistribution
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


def distribution_command(
    path: Annotated[str, typer.Argument(help="Path to the Delta table")],
        metric: Annotated[
        DistributionMetric,
        typer.Option(
            "--metric",
            "-m",
            help="Metric to analyze (default: file_size)",
        ),
    ] = DistributionMetric.FILE_SIZE,
) -> None:
    """Analyze distribution of file sizes and partitions of a Delta 
    Lake table for specified columns."""

    dist = distribution(path)
    
    console_header(console=console, title="Distribution Analysis - File Size")
    console_dist_statistics(dist=dist.distribution_files, metric="Size in MiB", console=console)
    console_dist_histogram(dist=dist.distribution_files, metric="Size in MiB", console=console)

    if dist.distribution_partitions:
        console_header(console=console, title="Distribution Analysis - Partition Size")
        console_dist_statistics(
            dist=dist.distribution_partitions, metric="Size in MiB", console=console
        )
        console_dist_histogram(
            dist=dist.distribution_partitions, metric="Size in MiB", console=console
        )
