Here’s a detailed, multi-level blueprint for implementing `delta-inspect`, followed by a series of code-generation LLM prompts — structured to support a test-driven, incremental, and integration-focused development process.

---

# 🧭 Phase 1: High-Level Blueprint

## 1. **Foundation**

* Project scaffolding & configuration
* Basic CLI structure via `typer`
* Set up `pyproject.toml`, dependencies, dev tools

## 2. **Core Engine**

* Implement `delta_reader`: reads `_delta_log/` JSON + Parquet checkpoint
* Reconstruct Delta Lake table snapshot (latest or versioned)
* Normalize schema, metadata

## 3. **Summary Command**

* Total file count, total size, estimated row count, schema snapshot
* Serve as integration baseline

## 4. **Filesizes Command**

* Compute stats + quantiles on file sizes
* Optional histogram and plotting

## 5. **Clustering Command**

* Analyze clustering using `minValues`, `maxValues` (if available)
* Fallback on `--columns` if schema lacks clustering keys

## 6. **Versions Command**

* Parse all commits
* List versions + timestamps

## 7. **Programmatic API**

* Wrap submodules as typed `pydantic` models with `.plot()` methods

## 8. **Testing and Integration**

* Full unit test suite with `pytest`
* Integration testing with `tmp_path`, mocks, CLI calls

---

# 🪜 Phase 2: Iterative Breakdown

## Milestone 0: Repository & Tooling

* Set up directory structure
* Add `pyproject.toml`
* Configure `pytest`, `pre-commit`, `mypy`, etc.

## Milestone 1: Implement `delta_reader`

* Read JSON logs and checkpoints
* Reconstruct latest state
* Normalize to DataFrame

## Milestone 2: CLI Bootstrapping

* Set up `typer` CLI with `summary` stub
* Wire core engine into CLI

## Milestone 3: Implement `summary`

* Add summary metrics: file count, size, schema
* Add basic unit test

## Milestone 4: Implement `filesizes`

* Compute file size stats, quantiles
* Add optional plotting
* Test core and CLI

## Milestone 5: Implement `clustering`

* Analyze clustering stats
* Validate against schema
* Plotting support

## Milestone 6: Implement `versions`

* List all commits + timestamps

## Milestone 7: API Exposure

* Typed models for programmatic use
* `.json()` and `.plot()` support

## Milestone 8: Final Integration & CLI Polish

* Add global options, error handling
* Rich output
* Final end-to-end tests

---

# 🧩 Phase 3: Micro-Steps

## Example: Milestone 1 — `delta_reader` Micro-Steps

1. Create `util/delta_reader.py`
2. Implement `find_latest_version(path)`
3. Implement `load_checkpoint(path, version)`
4. Implement `load_json_logs(path, start_version)`
5. Implement `reconstruct_table_state(path, version=None)`
6. Add unit test for each function using mock data
7. Validate schema extraction from snapshot

---

# 🤖 Phase 4: LLM Code Generation Prompts

## 🔧 Prompt 1: Scaffold Project

```text
Create a Python project with the following structure:

delta_inspect/
├── cli/
│   ├── __init__.py
├── util/
│   └── __init__.py
tests/
├── test_util_delta_reader.py

Include a `pyproject.toml` with dependencies:
- polars
- fsspec
- pydantic
- typer[all]
- rich
- plotly (optional dependency group `[viz]`)

Also include dev dependencies:
- pytest
- hypothesis
- pre-commit
```

---

## 🔧 Prompt 2: Implement `delta_reader.find_latest_version`

```text
Implement a function `find_latest_version(path: str) -> int` in `delta_inspect/util/delta_reader.py`.

It should:
- Read filenames from `_delta_log` directory
- Match `<version>.json` and `<version>.checkpoint.parquet`
- Return the highest version number found

Add tests using a temporary directory and mock files like:
- `_delta_log/00000000000000000010.json`
- `_delta_log/00000000000000000008.checkpoint.parquet`
```

---

## 🔧 Prompt 3: Implement Checkpoint Loader

```text
Add a function `load_checkpoint(path: str, version: int) -> polars.DataFrame`.

It should:
- Read `version.checkpoint.parquet` from `_delta_log`
- Return a `polars.DataFrame` of the checkpoint data
- Use `fsspec` to support remote/local paths

Write a test using a small synthetic checkpoint file (mocked with `fsspec.implementations.local.LocalFileSystem`)
```

---

## 🔧 Prompt 4: Load JSON Logs

```text
Implement `load_json_logs(path: str, start_version: int) -> list[dict]`.

It should:
- Iterate over all JSON files with version > `start_version`
- Return a list of parsed JSON objects

Test with a temporary `_delta_log/` directory and example JSON content.
```

---

## 🔧 Prompt 5: Build Table Snapshot

```text
Implement `reconstruct_table_state(path: str, version: Optional[int] = None) -> DeltaTableSnapshot`.

Steps:
- Call `find_latest_version()` if version is None
- Load checkpoint via `load_checkpoint()`
- Load trailing JSON logs and apply them
- Return an object with:
  - `add_files`: list of file metadata
  - `schema`: current schema as a dict
  - `version`: version number

Define `DeltaTableSnapshot` as a `pydantic.BaseModel`.
Test snapshot creation with fixture data.
```

---

## 🔧 Prompt 6: CLI + `summary` Hook

```text
Use `typer` to define a CLI group `delta-inspect`.

Add subcommand `summary` with flags:
- `--path PATH`
- `--json`
- `--verbose`

Inside the command:
- Load snapshot from `delta_reader`
- Print total file count, size, estimated row count
- If `--json` is passed, output as JSON

Write integration test using `subprocess.run()` with a test `_delta_log` path.
```

---

## 🔧 Prompt 7: Implement `FileSizeStats`

```text
Define `FileSizeStats(BaseModel)` in `filesizes/model.py` with:

- `total_files`, `total_size_bytes`
- `mean`, `median`, `stddev`, `min`, `max`
- `quantiles_bytes: Dict[str, float]`
- `histogram: List[HistogramBucket]`

Add `.plot()` method using Plotly
Add test with synthetic file size list
```

---

## 🔧 Prompt 8: Wire `filesizes` CLI

```text
Add a `filesizes` subcommand to the CLI.

Flags:
- `--path`
- `--json`
- `--plot`

Use snapshot from `delta_reader`, extract file sizes from `add_files`, compute stats, return `FileSizeStats`.

Render in terminal or as JSON/Plotly depending on flags.

Add end-to-end test for CLI invocation and plot output.
```

---

*(Repeat similar prompts for `clustering`, `versions`, error handling, rich output, and tests.)*

---

# ✅ Final Checklist

* [x] Each step builds on a previous one
* [x] Every output is tested or validated
* [x] No dangling code; CLI and API always wired
* [x] Complexity increases gradually
* [x] Test-driven at every level

---

Let me know if you'd like this exported to a doc, turned into an issue tracker plan (e.g. GitHub), or if you're ready to begin executing prompts.


Here is the refined and complete **Phase 4: LLM Code Generation Prompt Series** for `delta-inspect`. Each prompt is designed for incremental, test-driven implementation, with strict layering and no orphaned code.

---

## 📁 Prompt 1: Scaffold Project

```text
Create a Python project named `delta-inspect` with the following layout:

delta_inspect/
├── cli/
│   ├── filesizes.py
│   ├── clustering.py
│   ├── summary.py
│   └── versions.py
├── filesizes/
│   ├── core.py
│   ├── model.py
│   └── plot.py
├── clustering/
│   ├── core.py
│   ├── model.py
│   └── plot.py
├── summary/
│   ├── core.py
│   └── model.py
├── util/
│   ├── delta_reader.py
│   ├── fs.py
│   └── log.py
tests/
├── test_util_delta_reader.py

Include a `pyproject.toml` with:

[project]
name = "delta-inspect"
dependencies = [
    "polars",
    "fsspec",
    "pydantic",
    "typer[all]",
    "rich"
]
[project.optional-dependencies]
viz = ["plotly"]

Add dev dependencies:
- pytest
- hypothesis
- pre-commit
```

---

## 📁 Prompt 2: Implement `find_latest_version`

```text
Create a function `find_latest_version(path: str) -> int` in `delta_inspect/util/delta_reader.py`.

- List all files in the `_delta_log` subdirectory
- Match filenames like `00000000000000000001.json` or `.checkpoint.parquet`
- Parse version numbers and return the highest one

Also write a pytest function in `tests/test_util_delta_reader.py`:
- Create a temporary `_delta_log` dir
- Touch some files with versioned names
- Assert `find_latest_version()` returns the highest one
```

---

## 📁 Prompt 3: Implement `load_checkpoint`

```text
Add function `load_checkpoint(path: str, version: int) -> polars.DataFrame` to `delta_inspect/util/delta_reader.py`.

- Use `fsspec.open()` to read the Parquet file:
  `_delta_log/00000000000000000010.checkpoint.parquet`
- Use `polars.read_parquet()` to load it
- Return the DataFrame

Write a unit test using a fixture checkpoint (can be mocked or small actual Parquet file).
```

---

## 📁 Prompt 4: Implement `load_json_logs`

```text
Add function `load_json_logs(path: str, start_version: int) -> list[dict]` to `delta_inspect/util/delta_reader.py`.

- Iterate all files in `_delta_log/`
- Parse versions and load JSON files with version > start_version
- Use `json.loads()` line by line (Delta logs are JSONL)

Write a test with:
- Temporary `_delta_log/` dir
- Two mock JSON files: one older than `start_version`, one newer
- Validate that only the newer one is returned
```

---

## 📁 Prompt 5: Implement `DeltaTableSnapshot` model

```text
Create a `DeltaTableSnapshot` class in `delta_inspect/util/delta_reader.py`.

Use `pydantic.BaseModel`. Fields:
- `version: int`
- `schema: dict`
- `add_files: list[dict]`

Create dummy data and write a unit test to:
- Construct a `DeltaTableSnapshot`
- Serialize to JSON
- Validate field types
```

---

## 📁 Prompt 6: Build full `reconstruct_table_state`

```text
Add function `reconstruct_table_state(path: str, version: Optional[int] = None) -> DeltaTableSnapshot`.

Steps:
- If version is None, call `find_latest_version(path)`
- Load checkpoint for that version
- Load JSON logs after checkpoint
- Extract `add` actions and `metaData` (for schema)
- Assemble and return a `DeltaTableSnapshot`

Add a test that:
- Mocks out `load_checkpoint` and `load_json_logs`
- Checks the snapshot contains expected `add_files` and `schema`
```

---

## 🧪 Prompt 7: Add CLI with Typer and summary stub

````text
Set up a CLI in `delta_inspect/cli/summary.py` using `typer`.

Define a `summary` subcommand:
- Arguments: `--path`, `--json`, `--verbose`
- Call `reconstruct_table_state(path)`
- Print total file count and total bytes from `snapshot.add_files`

Add CLI entry in `delta_inspect/__main__.py`:
```python
import typer
from delta_inspect.cli.summary import summary

app = typer.Typer()
app.command("summary")(summary)

if __name__ == "__main__":
    app()
````

Add an integration test using `subprocess.run()` to invoke the CLI.

````

---

## 📊 Prompt 8: Define `FileSizeStats` Model

```text
Create `FileSizeStats` in `delta_inspect/filesizes/model.py`:

```python
class HistogramBucket(BaseModel):
    range_start: int
    range_end: int
    count: int

class FileSizeStats(BaseModel):
    total_files: int
    total_size_bytes: int
    mean_size_bytes: float
    median_size_bytes: float
    stddev_size_bytes: float
    min_size_bytes: int
    max_size_bytes: int
    quantiles_bytes: Dict[str, float]
    histogram: List[HistogramBucket]

    def plot(self): ...
````

Write unit tests:

* Instantiate with known data
* Check quantiles and histogram shape

````

---

## 📊 Prompt 9: Add filesizes computation logic

```text
In `delta_inspect/filesizes/core.py`, add `def analyze(path: str) -> FileSizeStats`.

Steps:
- Call `reconstruct_table_state(path)`
- Extract file sizes from `add_files`
- Use `polars` to compute stats
- Return `FileSizeStats`

Write a test that:
- Mocks snapshot with fake file sizes
- Validates output fields
````

---

## 📊 Prompt 10: Add filesizes CLI

```text
Create `delta_inspect/cli/filesizes.py`.

Add CLI command:
- `--path`, `--json`, `--plot`
- Call `analyze(path)`
- Output summary in terminal or `.json()`
- Call `.plot()` if `--plot` is used

Add CLI test:
- Feed mock path
- Check output format with `--json`
- Ensure `.plot()` runs without error
```

---

## 📌 Prompt 11: Implement `ClusteringReport`

````text
Create `ClusteringReport` in `delta_inspect/clustering/model.py`:

```python
class ClusteringReport(BaseModel):
    total_partition_count: int
    total_constant_partition_count: int
    average_overlaps: float
    average_depth: float
    partition_depth_histogram: List[HistogramBucket]

    def plot(self): ...
````

Add a test that:

* Instantiates with known values
* Tests `.plot()` runs and bucket contents are correct

````

---

## 📌 Prompt 12: Clustering core logic

```text
Add `analyze(path: str, columns: Optional[list[str]] = None) -> ClusteringReport` in `clustering/core.py`.

Steps:
- Load snapshot
- Validate clustering columns (raise if not found and no `--columns`)
- Compute partition depths and overlaps
- Return `ClusteringReport`

Write test with a mocked snapshot having `minValues`, `maxValues` for target columns.
````

---

## 📌 Prompt 13: Clustering CLI

```text
Add `delta_inspect/cli/clustering.py`.

Subcommand: `clustering`
- `--path`, `--columns`, `--plot`, `--json`, `--ignore-missing-stats`
- Use `analyze(path, columns)`
- Output report or `.plot()`

Test:
- CLI output with and without `--columns`
- Plot renders correctly
```

---

## 🕰️ Prompt 14: Versions Command

```text
Add `delta_inspect/cli/versions.py`.

Subcommand: `versions`
- `--path`, `--json`

Steps:
- List all `_delta_log` JSON files
- Extract version and timestamp from `commitInfo`
- Sort and display

Add test with fake `_delta_log` entries and mock timestamps.
```

---

## 🧪 Prompt 15: Integration Test Suite

```text
Write integration tests in `tests/cli/` for each subcommand:

- `summary`
- `filesizes`
- `clustering`
- `versions`

Use `tmp_path` and pre-copied `_delta_log` fixtures
Use `subprocess.run()` to validate CLI output
```



Here's a clear and concise **introduction message** for a code-generation LLM (e.g. Claude), providing just the right amount of context for the `delta-inspect` project and the prompt sequence:

---

```text
You are helping to build a Python CLI and library project called `delta-inspect`. It is a developer tool for inspecting Delta Lake tables *without using Spark*. The tool reads and analyzes metadata files (JSON transaction logs and Parquet checkpoints) stored in the `_delta_log/` directory of a Delta table.

Your goal is to implement the project incrementally using best practices: test-driven development, small composable steps, and CLI-first integration. The CLI is built with `typer`, and the data models use `pydantic`. The tool also supports plotting using `plotly` and file system abstraction via `fsspec`.

The final tool will include CLI subcommands like `summary`, `filesizes`, `clustering`, and `versions`, each exposing the same logic through a typed programmatic API.

You will be given a series of step-by-step prompts. Each prompt builds directly on the results of the previous one. All implementations should be:
- Fully functional and safe in isolation
- Covered by unit tests
- Designed to be reusable and extensible

Avoid orphaned code. Every component you implement should be immediately integrated and tested. Use mock data where needed, especially for reading files.

Start each task with a clean and pragmatic mindset, and aim for production-quality code in clear, idiomatic Python.
```

---

Let me know if you'd like versions of this adapted for use with GitHub Copilot Chat, OpenAI functions, or an automated LLM orchestrator.
