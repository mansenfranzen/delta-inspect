"""Main CLI application for delta-inspect."""

import typer
from delta_inspect.cli.summary import summary_command

app = typer.Typer(
    name="delta-inspect", 
    help="Inspect and analyze Delta Lake tables.",
    no_args_is_help=True
)

# Add subcommands
app.command("summary", help="Generate summary information for Delta Lake tables")(summary_command)

# Export for entry point
__all__ = ["app"]

if __name__ == "__main__":
    app()
