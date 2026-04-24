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

---

### Config File Usage

Example:

```bash
cargo run -- --file test.csv --config config.toml
```

CLI arguments override config values.

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

## Project Structure

```text
src/
  main.rs        → entry point
  cli.rs         → CLI argument parsing
  config.rs      → config file handling
  logging.rs     → logging setup
  types.rs       → shared data structures
  readers/
    csv.rs       → CSV profiling
    json.rs      → JSON profiling
    parquet.rs   → Parquet profiling

tests/
  profile_tests.rs → integration tests
```

---

## Logging & Error Handling

* Uses structured logging (`tracing`)
* Supports verbose mode
* Provides clear error messages for:

  * invalid files
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

## Current Status

Completed:

* Multi-format support (CSV, JSON, Parquet)
* Automated EDA metrics
* Data quality warnings
* CLI interface
* Config support
* Structured logging
* Initial test suite

---

## Work In Progress

* Additional edge-case tests
* Benchmarking
* Threading support (`--threads`)
* Progress tracking for large datasets
* Documentation improvements

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
