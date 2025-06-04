from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from pydantic import BaseModel

WIDTH_COL_FIRST = 30
WIDTH_TOTAL = 90
WIDTH_REMAIN = WIDTH_TOTAL - WIDTH_COL_FIRST


class TableColumn(BaseModel):
    title: str
    style: str | None = None
    width: int = 20


def console_header(console: Console, title: str):
    title = Panel(
        f"[bold blue]{title}[/bold blue]",
        title="📊 delta-inspect",
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


def format_number(num: int | float) -> str:
    """Format numbers with appropriate precision."""
    if isinstance(num, int):
        return f"{num:,}"
    return f"{num:.3f}"


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
