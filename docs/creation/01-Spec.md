See ChatGPT prompt [here](https://chatgpt.com/share/683a013a-46e4-800f-87dc-a44c83f4ff88).

---

# 📦 **delta-inspect**: Developer Specification

## ✨ Overview

`delta-inspect` is a Python package and CLI tool for introspecting **Delta Lake tables** *without requiring Spark*. It focuses on analyzing metadata to help users understand table health and structure — particularly file size distribution and clustering efficiency.

---

## ✅ Key Features

* Read and interpret Delta Lake metadata (JSON + Parquet checkpoint)
* CLI with subcommands for `summary`, `filesizes`, `clustering`, and `versions`
* Optional JSON and Plotly histogram outputs
* Pure Python, no `pyspark` dependency
* Programmatic API with typed `pydantic` results
* Designed for extensibility via `fsspec`

---

## 🧱 Requirements

### Input

* `--path`: Local path to a Delta Lake table directory

### Supported Metadata Sources

* `_delta_log/*.json` — Delta transaction logs
* `_delta_log/*.checkpoint.parquet` — Delta table checkpoints

---

## 🏗️ Architecture

### 📁 Project Structure

```
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
```

### 🧩 Core Dependencies

* `polars` – fast dataframe manipulation
* `plotly` – histogram plotting (optional extra `[viz]`)
* `fsspec` – abstracted filesystem access
* `pydantic` – structured results, with built-in `.json()` and `.plot()` methods
* `typer` – modern CLI interface
* `rich` – for terminal table formatting (optional)

---

## 🧪 CLI Design

### General Flags

* `--path PATH`
* `--version VERSION` (default: latest)
* `--json`
* `--plot`
* `--verbose`
* `--ignore-missing-stats`
* `--columns col1,col2` (only for `clustering`)

### Subcommands

#### `delta-inspect summary`

* Total files
* Total size
* Estimated row count
* Creation and last commit timestamps
* Latest schema preview

#### `delta-inspect filesizes`

* File count, total size
* Min, max, mean, median, stddev
* Quantiles: 5%, 25%, 75%, 95%
* Histogram of file sizes

#### `delta-inspect clustering`

* Uses clustering columns from latest schema, or via `--columns`
* Metrics:

  * `total_partition_count`
  * `total_constant_partition_count`
  * `average_overlaps`
  * `average_depth`
  * `partition_depth_histogram`

#### `delta-inspect versions`

* List all available Delta table versions with commit timestamps

---

## 🧠 Programmatic API

### Usage Example

```python
from delta_inspect.clustering import analyze as analyze_clustering
report = analyze_clustering(path="/path/to/table")
report.plot()  # Optional visualization
```

### Return Types

#### `filesizes.model.FileSizeStats`

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
    quantiles_bytes: Dict[str, float]  # e.g. {'p5': 123, ...}
    histogram: List[HistogramBucket]

    def plot(self): ...
```

#### `clustering.model.ClusteringReport`

```python
class ClusteringReport(BaseModel):
    total_partition_count: int
    total_constant_partition_count: int
    average_overlaps: float
    average_depth: float
    partition_depth_histogram: List[HistogramBucket]

    def plot(self): ...
```

---

## ⚙️ Data Handling

* Delta table is parsed by reconstructing the latest (or specified) snapshot
* Uses checkpoint + trailing JSON logs for efficient state reconstruction
* Clustering columns are taken from latest schema unless overridden
* If stats (`minValues`, `maxValues`) are missing:

  * Raise error unless `--ignore-missing-stats` is passed
* Missing clustering columns in schema → fail and request `--columns`

---

## ⚠️ Error Handling

| Condition                   | Behavior                                         |
| --------------------------- | ------------------------------------------------ |
| Invalid path                | Fail with `FileNotFoundError`                    |
| No `_delta_log` present     | Fail with custom `DeltaTableNotFound`            |
| Missing clustering stats    | Fail unless `--ignore-missing-stats` is provided |
| No clustering columns found | Fail and require `--columns`                     |
| Mixed schema files          | Use latest schema only, ignore outdated files    |

---

## 🧪 Testing Plan

### Unit Tests

* Each module (`filesizes`, `clustering`, `summary`) tested in isolation
* Use sample `_delta_log` files under `tests/data/`
* Use `pytest` and `hypothesis` for data edge case testing

### Integration Tests

* Validate CLI end-to-end using `subprocess` and `tmp_path`
* Test all output modes (`json`, human-readable, plot)

### Mocks/Stubs

* Mock file reads via `fsspec.implementations.local.LocalFileSystem`
* Abstract Delta table reading behind a testable interface

---

## 🧰 Packaging

### `pyproject.toml`

```toml
[project]
name = "delta-inspect"
version = "0.1.0"
dependencies = [
    "polars",
    "fsspec",
    "pydantic",
    "typer[all]",
    "rich"
]
[project.optional-dependencies]
viz = ["plotly"]

[tool.setuptools.packages.find]
include = ["delta_inspect*"]
```

---

## 🚀 Next Steps

1. Scaffold the repo using this structure
2. Implement `util.delta_reader` first
3. Build and test the `summary` command as a baseline
4. Add `filesizes`, then `clustering`
5. Finalize API/CLI symmetry and polish docs

