mod cli;
mod config;
mod logging;
mod readers;
mod types;

use std::time::Instant;

use clap::Parser;
use cli::Cli;
use config::{load_config, AppConfig};
use readers::csv::{preview_csv, profile_csv};
use readers::json::{preview_json, profile_json};
use readers::parquet::{preview_parquet, profile_parquet};
use tracing::{info, warn};
use types::InferredType;

fn main() {
    let start_time = Instant::now();

    let args = Cli::parse();

    let file_path = args.file.clone();

    let config = match &args.config {
        Some(path) => match load_config(path) {
            Ok(cfg) => cfg,
            Err(e) => {
                eprintln!("Failed to load config file '{}': {}", path, e);
                std::process::exit(1);
            }
        },
        None => AppConfig::default(),
    };

    let verbose = if args.verbose {
        true
    } else {
        config.verbose.unwrap_or(false)
    };

    logging::init_logging(verbose);

    let format = args
        .format
        .clone()
        .or(config.format.clone())
        .unwrap_or_else(|| detect_format(&file_path));

    let delimiter = args
        .delimiter
        .or(config.delimiter)
        .unwrap_or(',');

    let dry_run = if args.dry_run {
        true
    } else {
        config.dry_run.unwrap_or(false)
    };

    info!("Starting dataset profiler");
    info!("Input file: {}", file_path);
    info!("Detected format: {}", format);

    if dry_run {
        info!("Dry-run mode enabled");

        match format.as_str() {
            "csv" => match preview_csv(&file_path, delimiter as u8) {
                Ok(preview) => {
                    let elapsed = start_time.elapsed().as_secs_f64();

                    println!("Dataset Profiler Dry Run");
                    println!("------------------------");
                    println!("File: {}", preview.file_path);
                    println!("Format: csv");
                    println!("Delimiter: {}", delimiter);
                    println!("Columns: {}", preview.column_count);
                    println!("Headers: {:?}", preview.headers);
                    println!("Time Taken: {:.4} seconds", elapsed);
                    println!("Dry run complete. Full profiling was skipped.");
                }
                Err(e) => {
                    eprintln!("Error during dry run: {}", e);
                    std::process::exit(1);
                }
            },
            "json" | "jsonl" | "ndjson" => match preview_json(&file_path) {
                Ok(preview) => {
                    let elapsed = start_time.elapsed().as_secs_f64();

                    println!("Dataset Profiler Dry Run");
                    println!("------------------------");
                    println!("File: {}", preview.file_path);
                    println!("Format: {}", format);
                    println!("Columns: {}", preview.column_count);
                    println!("Keys: {:?}", preview.keys);
                    println!("Time Taken: {:.4} seconds", elapsed);
                    println!("Dry run complete. Full profiling was skipped.");
                }
                Err(e) => {
                    eprintln!("Error during dry run: {}", e);
                    std::process::exit(1);
                }
            },
            "parquet" => match preview_parquet(&file_path) {
                Ok(preview) => {
                    let elapsed = start_time.elapsed().as_secs_f64();

                    println!("Dataset Profiler Dry Run");
                    println!("------------------------");
                    println!("File: {}", preview.file_path);
                    println!("Format: parquet");
                    println!("Columns: {}", preview.column_count);
                    println!("Columns/Fields: {:?}", preview.columns);
                    println!("Time Taken: {:.4} seconds", elapsed);
                    println!("Dry run complete. Full profiling was skipped.");
                }
                Err(e) => {
                    eprintln!("Error during dry run: {}", e);
                    std::process::exit(1);
                }
            },
            _ => {
                eprintln!("Unsupported format: {}", format);
                std::process::exit(1);
            }
        }

        return;
    }

    match format.as_str() {
        "csv" => match profile_csv(&file_path, delimiter as u8) {
            Ok(profile) => {
                info!("CSV profiling completed successfully");

                let valid_row_count = profile.row_count.saturating_sub(profile.malformed_row_count);
                let average_row_width = if valid_row_count > 0 {
                    profile.total_row_width as f64 / valid_row_count as f64
                } else {
                    0.0
                };

                let elapsed = start_time.elapsed().as_secs_f64();

                println!("CSV Profile Summary");
                println!("-------------------");
                println!("File: {}", profile.file_path);
                println!("Format: csv");
                println!("Delimiter: {}", delimiter);
                println!("Rows: {}", profile.row_count);
                println!("Columns: {}", profile.column_count);
                println!("Malformed Rows: {}", profile.malformed_row_count);
                println!("Average Row Width: {:.2} characters", average_row_width);
                println!("Time Taken: {:.4} seconds", elapsed);
                println!();

                if !profile.malformed_rows.is_empty() {
                    warn!("Malformed rows detected: {}", profile.malformed_row_count);

                    println!("Malformed Row Details:");
                    for bad_row in &profile.malformed_rows {
                        println!(
                            "row {} -> expected {} fields, found {}",
                            bad_row.row_number, bad_row.expected_fields, bad_row.found_fields
                        );
                    }
                    println!();
                }

                print_column_stats(profile.columns);
            }
            Err(e) => {
                eprintln!("Error profiling CSV: {}", e);
                std::process::exit(1);
            }
        },
        "json" | "jsonl" | "ndjson" => match profile_json(&file_path) {
            Ok(profile) => {
                info!("JSON profiling completed successfully");

                let valid_row_count = profile.row_count.saturating_sub(profile.malformed_row_count);
                let average_row_width = if valid_row_count > 0 {
                    profile.total_row_width as f64 / valid_row_count as f64
                } else {
                    0.0
                };

                let elapsed = start_time.elapsed().as_secs_f64();

                println!("JSON Profile Summary");
                println!("--------------------");
                println!("File: {}", profile.file_path);
                println!("Format: {}", format);
                println!("Rows: {}", profile.row_count);
                println!("Columns: {}", profile.column_count);
                println!("Malformed Rows: {}", profile.malformed_row_count);
                println!("Average Row Width: {:.2} characters", average_row_width);
                println!("Time Taken: {:.4} seconds", elapsed);
                println!();

                if !profile.malformed_rows.is_empty() {
                    warn!("Malformed JSON records detected: {}", profile.malformed_row_count);

                    println!("Malformed Record Details:");
                    for row_num in &profile.malformed_rows {
                        println!("record {} -> invalid JSON object structure", row_num);
                    }
                    println!();
                }

                print_column_stats(profile.columns);
            }
            Err(e) => {
                eprintln!("Error profiling JSON: {}", e);
                std::process::exit(1);
            }
        },
        "parquet" => match profile_parquet(&file_path) {
            Ok(profile) => {
                info!("Parquet profiling completed successfully");

                let average_row_width = if profile.row_count > 0 {
                    profile.total_row_width as f64 / profile.row_count as f64
                } else {
                    0.0
                };

                let elapsed = start_time.elapsed().as_secs_f64();

                println!("Parquet Profile Summary");
                println!("-----------------------");
                println!("File: {}", profile.file_path);
                println!("Format: parquet");
                println!("Rows: {}", profile.row_count);
                println!("Columns: {}", profile.column_count);
                println!("Average Row Width: {:.2} characters", average_row_width);
                println!("Time Taken: {:.4} seconds", elapsed);
                println!();

                print_column_stats(profile.columns);
            }
            Err(e) => {
                eprintln!("Error profiling Parquet: {}", e);
                std::process::exit(1);
            }
        },
        _ => {
            eprintln!("Unsupported format: {}", format);
            std::process::exit(1);
        }
    }
}

fn print_column_stats(columns: Vec<types::ColumnProfile>) {
    println!("Column Stats:");
    for col in columns {
        let avg_length = if col.non_null_count > 0 {
            col.total_length as f64 / col.non_null_count as f64
        } else {
            0.0
        };

        match col.inferred_type {
            InferredType::Integer | InferredType::Float => {
                println!(
                    "{} -> type: {}, nulls: {}, total: {}, min: {}, max: {}",
                    col.name,
                    display_type(&col.inferred_type),
                    col.null_count,
                    col.total_count,
                    format_optional_f64(col.numeric_min),
                    format_optional_f64(col.numeric_max)
                );
            }
            InferredType::String | InferredType::Mixed => {
                println!(
                    "{} -> type: {}, nulls: {}, total: {}, min_len: {}, max_len: {}, avg_len: {:.2}",
                    col.name,
                    display_type(&col.inferred_type),
                    col.null_count,
                    col.total_count,
                    format_optional_usize(col.min_length),
                    format_optional_usize(col.max_length),
                    avg_length
                );
            }
            _ => {
                println!(
                    "{} -> type: {}, nulls: {}, total: {}",
                    col.name,
                    display_type(&col.inferred_type),
                    col.null_count,
                    col.total_count
                );
            }
        }
    }
}

fn detect_format(file_path: &str) -> String {
    if let Some(ext) = std::path::Path::new(file_path)
        .extension()
        .and_then(|e| e.to_str())
    {
        ext.to_lowercase()
    } else {
        "unknown".to_string()
    }
}

fn display_type(t: &InferredType) -> &'static str {
    match t {
        InferredType::Unknown => "unknown",
        InferredType::Integer => "integer",
        InferredType::Float => "float",
        InferredType::Boolean => "boolean",
        InferredType::String => "string",
        InferredType::Mixed => "mixed",
    }
}

fn format_optional_f64(value: Option<f64>) -> String {
    match value {
        Some(v) => format!("{:.2}", v),
        None => "N/A".to_string(),
    }
}

fn format_optional_usize(value: Option<usize>) -> String {
    match value {
        Some(v) => v.to_string(),
        None => "N/A".to_string(),
    }
}