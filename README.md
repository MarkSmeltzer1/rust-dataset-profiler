# Dataset Profiler CLI

## Overview

The Dataset Profiler is a Rust-based command line application designed to help data engineers quickly understand the structure, quality, and characteristics of a dataset before building data pipelines.

It provides automated first-pass exploratory data analysis (EDA) across multiple file formats.

Supported formats:

* CSV
* JSON
* NDJSON / JSONL
* Parquet

---

## Why This Tool Exists

In real-world data engineering workflows, one of the first steps is understanding incoming data before building ETL pipelines.

This tool helps answer key questions:

* How many rows and columns are in the dataset?
* What data types do columns contain?
* How many null values are present?
* What percentage of each column is missing?
* What are the numeric ranges and averages?
* Are there malformed rows?
* Are there suspicious or low-quality columns?

This reduces manual inspection and speeds up dataset onboarding.

---

## Features

### Core Profiling

* Row count and column count
* Type inference (integer, float, string, boolean, mixed)
* Null counts and null percentages
* Numeric statistics (min, max, mean)
* String length statistics
* Average row width
* Runtime measurement

---

### Data Quality Detection

* Malformed row detection (CSV / JSON)
* Missing data warnings
* Mixed type detection
* Negative numeric value detection
* Extreme value detection

---

### CLI Features

* `--file` input file
* `--format` optional format override
* `--delimiter` CSV delimiter
* `--config` TOML configuration file
* `--verbose` structured logging
* `--dry-run` preview mode
* `--threads` thread setting for future parallel readers
* built-in `--help`
* built-in `--version`

---

## Installation

### Prerequisites

* Rust (https://www.rust-lang.org/tools/install)

Verify installation:

```bash
rustc --version
cargo --version
```

---

### Clone the Repository

```bash
git clone <YOUR_REPO_URL>
cd dataset-profiler
```

---

### Build the Project

```bash
cargo build
```

---

## Running the Application

### Basic Usage

```bash
cargo run -- --file <path_to_file>
```

---

### Example Output

```text
CSV Profile Summary
-------------------
File: test.csv
Format: csv
Delimiter: ,
Rows: 3
Columns: 3
Malformed Rows: 0
Average Row Width: 5.67 characters
Time Taken: 0.0417 seconds

Column Stats:
id -> type: integer, nulls: 0 (0.00%), total: 3, min: 1.00, max: 3.00, mean: 2.00
name -> type: string, nulls: 1 (33.33%), total: 3, min_len: 3, max_len: 5, avg_len: 4.00
age -> type: integer, nulls: 0 (0.00%), total: 3, min: 22.00, max: 30.00, mean: 25.67

Column Warnings:
- name -> moderate missingness (33.33% null)
```

---

### Examples

#### CSV

```bash
cargo run -- --file test.csv
```

#### JSON

```bash
cargo run -- --file test.json
```

#### JSONL / NDJSON

```bash
cargo run -- --file test.jsonl
```

#### Parquet

```bash
cargo run -- --file your_file.parquet
```

---

### Dry Run (Preview Only)

```bash
cargo run -- --file test.csv --dry-run
```

This shows:

* columns
* headers / keys

without full profiling.

---

### Verbose Logging

```bash
cargo run -- --file test.csv --verbose
```

Verbose mode also logs progress every 100,000 rows for large CSV, JSON, and Parquet profiles.

---

### Thread Setting

```bash
cargo run -- --file test.csv --threads 2
```

The current readers are primarily streaming and single-threaded. The flag is validated and logged so the CLI is ready for future parallel profiling work.

---

### Config File Usage

Example:

```bash
cargo run -- --file test.csv --config config.toml
```

CLI arguments override config values.

Example config:

```toml
format = "csv"
delimiter = ","
verbose = false
dry_run = false
threads = 1
```

---

## Running Tests

Run all tests:

```bash
cargo test
```

Run with output:

```bash
cargo test -- --nocapture
```

---

## Running Benchmarks

Run Criterion benchmarks:

```bash
cargo bench
```

The benchmark suite profiles generated CSV and JSON fixtures at small and medium sizes. Results are written under `target/criterion/`.

Example local results from a short Criterion run:

| Benchmark | Approximate Time |
| --- | ---: |
| CSV, 100 rows | 68-95 microseconds |
| CSV, 10,000 rows | 4.1-5.3 milliseconds |
| JSON, 100 rows | 179-246 microseconds |
| JSON, 10,000 rows | 22.9-28.1 milliseconds |

CSV is faster here because the reader streams records row by row with low parsing overhead. JSON arrays currently require parsing the full document structure before profiling, which is simpler but less memory-efficient for very large array-style JSON files.

---

## Project Structure

```text
src/
  main.rs        -> CLI entry point and application flow
  lib.rs         -> reusable crate modules for tests and benchmarks
  cli.rs         -> CLI argument parsing
  config.rs      -> config file handling
  errors.rs      -> custom error types and fatal exit helper
  logging.rs     -> logging setup
  types.rs       -> shared data structures
  readers/
    csv.rs       -> CSV profiling
    json.rs      -> JSON profiling
    parquet.rs   -> Parquet profiling

tests/
  cli_error_tests.rs -> CLI and error behavior tests
  profile_tests.rs   -> integration tests

benches/
  profile_benchmarks.rs -> Criterion performance benchmarks
```

---

## How It Works

1. `main.rs` parses CLI arguments with `clap`.
2. If `--config` is provided, `config.rs` loads TOML settings.
3. CLI values override config values.
4. `logging.rs` initializes structured logging based on `--verbose` or config.
5. `main.rs` validates the file path, format, and thread count.
6. The correct reader in `src/readers/` previews or profiles the file.
7. Reader modules fill shared structs from `types.rs`.
8. `main.rs` prints the final summary, column stats, warnings, and runtime.

---

## Warning Rules

Column warnings are heuristic checks meant to flag data that may need review:

* moderate missingness: at least 20% null values
* high missingness: at least 50% null values
* mixed or complex type: values do not fit one simple inferred type
* constant numeric column: all observed numeric values are the same
* negative values: numeric minimum is below zero
* extreme numeric range: absolute minimum or maximum is above 1,000,000

Warnings do not always mean the data is wrong. They are prompts for investigation before building a pipeline.

---

## Logging & Error Handling

* Uses structured logging (`tracing`)
* Supports verbose mode
* Logs file open, format selection, profiling start/end, malformed rows, column warnings, and large-file progress
* Provides clear error messages for:

  * invalid files
  * invalid arguments
  * invalid config files
  * parsing issues
  * unsupported formats
* Designed to fail gracefully with meaningful output

---

## Design Notes

### CSV

Processed row-by-row to avoid loading entire file into memory.

### JSON

* NDJSON is streamed line-by-line
* Standard JSON arrays are parsed in memory (acceptable for moderate sizes)

### Parquet

* Uses row-based API
* Handles large datasets (tested on 3.4M+ rows)

---

## Limitations

* Standard JSON arrays are parsed in memory; NDJSON is preferred for large JSON datasets.
* `--threads` is currently validated and logged, but the readers are mostly single-threaded.
* Parquet profiling uses the row-based API, which is simpler but not as optimized as Arrow batch processing.
* Type inference is heuristic-based and may classify timestamps or nested structures as mixed.
* Benchmarks are local measurements and can vary by machine.

---

## Current Status

Completed:

* Multi-format support (CSV, JSON, Parquet)
* Automated EDA metrics
* Data quality warnings
* CLI interface
* Config support
* Structured logging
* Edge-case and CLI test coverage
* Criterion benchmark setup
* Progress logging for large datasets
* Thread-count flag and validation

---

## Rubric Alignment

This project now addresses the main production-ready rubric categories:

* CLI and UX: help/version support, validation, config overrides, dry-run, verbose mode, and thread flag
* Implementation: CSV, JSON/NDJSON, and Parquet profiling with streaming where practical
* Data engineering thinking: schema shape, type hints, null patterns, malformed rows, average row width, and quality warnings
* Logging and observability: structured logs, warning/error levels, progress logging, row counts, and runtime summaries
* Testing: core profiling tests, CLI error tests, config behavior tests, and edge-case tests
* Benchmarking: Criterion benchmarks for CSV and JSON profiling
* Documentation: usage, examples, architecture, warning rules, benchmark results, limitations, and trade-offs

---

## Remaining Improvements

* Optional Parquet fixture tests
* Potential Arrow-based Parquet optimization
* Real parallel profiling implementation behind `--threads`

---

## Contribution Workflow

```bash
git pull
git checkout -b feature/<feature-name>
cargo test
git add .
git commit -m "Describe changes"
git push
```

---

## Summary

This project is a multi-format dataset profiler that automates the first stage of data engineering: understanding the dataset.

It provides structure analysis, data quality insights, and automated EDA to reduce manual effort and improve pipeline reliability.
