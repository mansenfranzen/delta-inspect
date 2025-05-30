`delta-inspect` is a Python package and CLI tool for introspecting **Delta Lake tables** *without requiring Spark*. It focuses on analyzing metadata to help users understand table health and structure — particularly file size distribution and clustering efficiency.

---

## ✅ Key Features

* Read and interpret Delta Lake metadata (JSON + Parquet checkpoint)
* CLI with subcommands for `summary`, `filesizes`, `clustering`, and `versions`
* Optional JSON and Plotly histogram outputs
* Pure Python, no `pyspark` dependency
* Programmatic API with typed `pydantic` results
* Designed for extensibility via `fsspec`
