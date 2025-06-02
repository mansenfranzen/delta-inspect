---
applyTo: '**'
---
You are helping to build a Python CLI and library project called `delta-inspect`. It is a developer tool for inspecting Delta Lake tables *without using Spark*. The tool reads and analyzes metadata files (JSON transaction logs and Parquet checkpoints) stored in the `_delta_log/` directory of a Delta table.

Your goal is to implement the project incrementally using best practices: test-driven development, small composable steps, and CLI-first integration. The CLI is built with `typer`, and the data models use `pydantic`. The tool also supports plotting using `plotly` and file system abstraction via `fsspec`.

The final tool will include CLI subcommands like `summary`, `filesizes`, `clustering`, and `versions`, each exposing the same logic through a typed programmatic API.

You will be given a series of step-by-step prompts. Each prompt builds directly on the results of the previous one. All implementations should be:
- Fully functional and safe in isolation
- Covered by unit tests
- Designed to be reusable and extensible

Avoid orphaned code. Every component you implement should be immediately integrated and tested. Use mock data where needed, especially for reading files.

Start each task with a clean and pragmatic mindset, and aim for production-quality code in clear, idiomatic Python.