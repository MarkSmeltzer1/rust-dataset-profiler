use clap::Parser;

/// Dataset Profiler CLI
#[derive(Parser, Debug)]
#[command(name = "dprofile")]
#[command(about = "Profile datasets (CSV, JSON, Parquet)")]
#[command(version = "0.1.0")]
pub struct Cli {
    /// Path to input file
    #[arg(short, long)]
    pub file: String,

    /// File format (csv, json, parquet)
    #[arg(long)]
    pub format: Option<String>,

    /// CSV delimiter
    #[arg(long, default_value = ",")]
    pub delimiter: char,

    /// Enable verbose logging
    #[arg(long, default_value_t = false)]
    pub verbose: bool,

    /// Dry run (no full processing)
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
}