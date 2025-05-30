# 🧰 delta-inspect

A lightweight Python CLI and API toolbox for **inspecting Delta Lake tables** without requiring Spark.

---

## ✨ Features

- 🔍 Analyze **file size distribution** and clustering quality using Delta Lake metadata only
- 📊 Output human-readable summaries, JSON, or **interactive Plotly histograms**
- 🐍 Use as a **CLI** or a Python **programmatic API**
- 🚫 **No Spark or heavy dependencies** — just fast, modern Python
- 🌐 Designed to support remote filesystems via `fsspec`

---

## 📦 Installation

```bash
# Core features
pip install delta-inspect

# For plotting support
pip install delta-inspect[viz]
````

---

## 🚀 Quickstart

```bash
delta-inspect summary --path /data/my_table
delta-inspect filesizes --path /data/my_table --plot
delta-inspect clustering --path /data/my_table --columns user_id,timestamp --json
```

Or use in Python:

```python
from delta_inspect.filesizes import analyze

report = analyze("/data/my_table")
print(report.mean_size_bytes)
report.plot()  # Plotly histogram
```

---

## 🧠 CLI Commands

### `summary`

Get general metadata overview of the table.

```bash
delta-inspect summary --path /data/my_table
```

### `filesizes`

Analyze Parquet file size distribution.

```bash
delta-inspect filesizes --path /data/my_table --plot
```

Outputs:

* Total file count and size
* Min, max, mean, median, stddev
* Quantiles (5%, 25%, 75%, 95%)
* Histogram (console or plot)

### `clustering`

Evaluate clustering health of Delta table.

```bash
delta-inspect clustering --path /data/my_table --columns region,timestamp
```

Metrics:

* Total partitions and constant partitions
* Average overlaps and depth
* Partition overlap histogram

### `versions`

List available table versions and their timestamps.

```bash
delta-inspect versions --path /data/my_table
```

---

## 🧩 Python API

Fully aligned with the CLI — structured outputs via Pydantic models.

```python
from delta_inspect.clustering import analyze

report = analyze("/data/my_table", columns=["region", "user_id"])
print(report.average_overlaps)
report.plot()
```

---

## 📁 Project Structure

```
delta_inspect/
├── cli/            # CLI entrypoints
├── filesizes/      # File size logic and models
├── clustering/     # Clustering analysis
├── summary/        # Table summary logic
├── util/           # Delta log reader, fs abstraction, logging
```

---

## 🧪 Testing

Run the full test suite:

```bash
pytest tests/
```

Tests include:

* Unit tests for each module
* CLI integration tests
* Delta log parsing and error handling

---

## ⚠️ Limitations

* Currently supports **local file paths** only — remote via `fsspec` is planned
* Depends on **Delta Lake metadata only** — does not inspect actual Parquet data
* Schema evolution is supported by using the **latest schema snapshot**

---

## 🛠️ Roadmap

* [ ] Add S3/remote path support via `fsspec`
* [ ] Support partitioned tables more deeply
* [ ] Add output export options (CSV, HTML report)
* [ ] Structured logging for pipelines

---

## 👥 Contributing

Contributions welcome! Please open an issue or pull request.

