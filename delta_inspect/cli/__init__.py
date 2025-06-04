"""Main CLI application for delta-inspect."""

import typer
from delta_inspect.cli.summary import summary_command
from delta_inspect.cli.clustering import clustering_command

app = typer.Typer(
    name="delta-inspect", 
    help="Inspect and analyze Delta Lake tables.",
    no_args_is_help=True
)

# Add subcommands
app.command("summary")(summary_command)
app.command("clustering")(clustering_command)

# Export for entry point
__all__ = ["app"]

if __name__ == "__main__":
    app()
