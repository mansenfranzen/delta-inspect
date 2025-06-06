from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.align import Align
from pydantic import BaseModel

from delta_inspect.util.model import BaseDistribution

WIDTH_COL_FIRST = 30
WIDTH_TOTAL = 90
WIDTH_REMAIN = WIDTH_TOTAL - WIDTH_COL_FIRST


class TableColumn(BaseModel):
    title: str
    style: str | None = None
    width: int = 20


def console_header(console: Console, title: str):
    title = Panel(
        renderable=Align(title, align="center"),
        title=f"📊 delta-inspect",
        width=90,
        style="bold blue",
        padding=1
    )

    console.print(title)
    console.print()


def console_table(
    console: Console, title: str, columns: list[TableColumn], rows: list[list]
):
    metadata_table = Table(title=title, show_header=True, header_style="bold magenta")
    for column in columns:
        metadata_table.add_column(column.title, style=column.style, width=column.width)
    for row in rows:
        metadata_table.add_row(*[str(item) for item in row])

    console.print(metadata_table)
    console.print()


def format_number(val: int | float | str) -> str:
    """Format numbers with appropriate precision."""
    if isinstance(val, int):
        return f"{val:,}"
    elif isinstance(val, float):
        return f"{val:.3f}"
    else:
        return val


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


def console_dist_histogram(dist: BaseDistribution, metric: str, console: Console):
    """Create a Rich table for the histogram of overlap counts."""

    columns = [
        TableColumn(title=metric, style="cyan", width=15),
        TableColumn(title="Count", style="white", width=10),
        TableColumn(title="Percentage", style="white", width=12),
        TableColumn(title="Bar", style="green"),
    ]

    count_total = sum(dist.hist.cnts)
    count_max = max(dist.hist.cnts)
    bin_count = len(dist.hist.bins)

    rows = []
    for idx, (bin_current, count) in enumerate(zip(dist.hist.bins, dist.hist.cnts)):
        if idx == 0:
            range_str = f"[0-{bin_current})"
        elif idx < bin_count - 1:
            bin_previous = dist.hist.bins[idx - 1]
            range_str = f"[{bin_previous}-{bin_current})"
        else:
            bin_previous = dist.hist.bins[idx - 1]
            range_str = f"[{bin_previous}-∞"

        percentage = (count / count_total * 100) if count_total > 0 else 0
        bar_length = int((count / count_max) * 20) if count_max > 0 else 0
        bar = "█" * bar_length

        rows.append([range_str, format_number(count), f"{percentage:.1f}%", bar])

    console_table(
        console=console, title="Distribution - Histogram", columns=columns, rows=rows
    )


def console_dist_statistics(dist: BaseDistribution, metric: str, console: Console):
    columns = [
        TableColumn(title="Metric", style="cyan", width=WIDTH_COL_FIRST),
        TableColumn(title=metric, style="white", width=30),
    ]

    rows = [
        ["Minimum", dist.min],
        ["5th Percentile", dist.q05],
        ["25th Percentile", dist.q25],
        ["Median (50th)", dist.q50],
        ["75th Percentile", dist.q75],
        ["95th Percentile", dist.q95],
        ["Maximum", dist.max],
        ["", ""],
        ["Mean", dist.mean],
        ["Std Deviation", dist.std],
    ]

    if isinstance(dist.min, (int, float)):
        rows = [[row[0], format_number(row[1])] for row in rows]

    console_table(
        console=console, title="Distribution - Statistics", columns=columns, rows=rows
    )