use std::path::Path;
use std::time::Instant;

use clap::Parser;
use dataset_profiler::cli::Cli;
use dataset_profiler::config::{load_config, AppConfig};
use dataset_profiler::errors::{fatal, ProfilerError};
use dataset_profiler::logging;
use dataset_profiler::readers::csv::{preview_csv, profile_csv};
use dataset_profiler::readers::json::{preview_json, profile_json};
use dataset_profiler::readers::parquet::{preview_parquet, profile_parquet};
use dataset_profiler::types::{ColumnProfile, InferredType};

fn main() {
    let start_time = Instant::now();
    let args = Cli::parse();
    let file_path = args.file.clone();

    let config = load_app_config(&args);
    let verbose = args.verbose || config.verbose.unwrap_or(false);
    logging::init_logging(verbose);

    if args.config.is_some() {
        logging::log_config_loaded(args.config.as_deref().unwrap());
    }

    if !Path::new(&file_path).exists() {
        fatal(ProfilerError::FileNotFound { path: file_path });
    }

    let explicit_format = args.format.clone().or(config.format.clone());
    let format_is_explicit = explicit_format.is_some();
    let format = explicit_format.unwrap_or_else(|| detect_format(&file_path));
    let delimiter = args.delimiter.or(config.delimiter).unwrap_or(',');
    let dry_run = args.dry_run || config.dry_run.unwrap_or(false);
    let threads = args.threads.or(config.threads).unwrap_or(1);

    if threads == 0 {
        fatal(ProfilerError::InvalidArgument {
            name: "--threads".to_string(),
            message: "must be at least 1".to_string(),
        });
    }

    logging::log_file_open(&file_path);
    logging::log_format(&format, format_is_explicit);
    logging::log_threads(threads);

    if dry_run {
        run_dry_run(&file_path, &format, delimiter, start_time);
        return;
    }

    run_full_profile(&file_path, &format, delimiter, start_time);
}

fn load_app_config(args: &Cli) -> AppConfig {
    match &args.config {
        Some(path) => match load_config(path) {
            Ok(cfg) => cfg,
            Err(e) => fatal(ProfilerError::ConfigError {
                path: path.clone(),
                message: e.to_string(),
            }),
        },
        None => AppConfig::default(),
    }
}

fn run_dry_run(file_path: &str, format: &str, delimiter: char, start_time: Instant) {
    logging::log_dry_run_start(file_path);

    match format {
        "csv" => match preview_csv(file_path, delimiter as u8) {
            Ok(preview) => {
                let elapsed = start_time.elapsed().as_secs_f64();
                logging::log_dry_run_complete(elapsed);

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
            Err(e) => fatal(ProfilerError::CsvParseError {
                path: file_path.to_string(),
                message: e.to_string(),
            }),
        },
        "json" | "jsonl" | "ndjson" => match preview_json(file_path) {
            Ok(preview) => {
                let elapsed = start_time.elapsed().as_secs_f64();
                logging::log_dry_run_complete(elapsed);

                println!("Dataset Profiler Dry Run");
                println!("------------------------");
                println!("File: {}", preview.file_path);
                println!("Format: {}", format);
                println!("Columns: {}", preview.column_count);
                println!("Keys: {:?}", preview.keys);
                println!("Time Taken: {:.4} seconds", elapsed);
                println!("Dry run complete. Full profiling was skipped.");
            }
            Err(e) => fatal(ProfilerError::JsonParseError {
                path: file_path.to_string(),
                message: e.to_string(),
            }),
        },
        "parquet" => match preview_parquet(file_path) {
            Ok(preview) => {
                let elapsed = start_time.elapsed().as_secs_f64();
                logging::log_dry_run_complete(elapsed);

                println!("Dataset Profiler Dry Run");
                println!("------------------------");
                println!("File: {}", preview.file_path);
                println!("Format: parquet");
                println!("Columns: {}", preview.column_count);
                println!("Columns/Fields: {:?}", preview.columns);
                println!("Time Taken: {:.4} seconds", elapsed);
                println!("Dry run complete. Full profiling was skipped.");
            }
            Err(e) => fatal(ProfilerError::ParquetReadError {
                path: file_path.to_string(),
                message: e.to_string(),
            }),
        },
        other => fatal(ProfilerError::UnsupportedFormat {
            format: other.to_string(),
        }),
    }
}

fn run_full_profile(file_path: &str, format: &str, delimiter: char, start_time: Instant) {
    match format {
        "csv" => {
            logging::log_profile_start(file_path, "CSV");

            match profile_csv(file_path, delimiter as u8) {
                Ok(profile) => {
                    let elapsed = start_time.elapsed().as_secs_f64();
                    logging::log_malformed_rows(profile.malformed_row_count);
                    logging::log_profile_complete(file_path, profile.row_count, elapsed);

                    let valid_row_count = profile
                        .row_count
                        .saturating_sub(profile.malformed_row_count);
                    let average_row_width = if valid_row_count > 0 {
                        profile.total_row_width as f64 / valid_row_count as f64
                    } else {
                        0.0
                    };

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
                        println!("Malformed Row Details:");
                        for bad_row in &profile.malformed_rows {
                            println!(
                                "row {} -> expected {} fields, found {}",
                                bad_row.row_number, bad_row.expected_fields, bad_row.found_fields
                            );
                        }
                        println!();
                    }

                    print_column_stats(&profile.columns);
                    print_column_warnings(&profile.columns);
                }
                Err(e) => fatal(ProfilerError::CsvParseError {
                    path: file_path.to_string(),
                    message: e.to_string(),
                }),
            }
        }
        "json" | "jsonl" | "ndjson" => {
            logging::log_profile_start(file_path, "JSON");

            match profile_json(file_path) {
                Ok(profile) => {
                    let elapsed = start_time.elapsed().as_secs_f64();
                    logging::log_malformed_rows(profile.malformed_row_count);
                    logging::log_profile_complete(file_path, profile.row_count, elapsed);

                    let valid_row_count = profile
                        .row_count
                        .saturating_sub(profile.malformed_row_count);
                    let average_row_width = if valid_row_count > 0 {
                        profile.total_row_width as f64 / valid_row_count as f64
                    } else {
                        0.0
                    };

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
                        println!("Malformed Record Details:");
                        for row_num in &profile.malformed_rows {
                            println!("record {} -> invalid JSON object structure", row_num);
                        }
                        println!();
                    }

                    print_column_stats(&profile.columns);
                    print_column_warnings(&profile.columns);
                }
                Err(e) => fatal(ProfilerError::JsonParseError {
                    path: file_path.to_string(),
                    message: e.to_string(),
                }),
            }
        }
        "parquet" => {
            logging::log_profile_start(file_path, "Parquet");

            match profile_parquet(file_path) {
                Ok(profile) => {
                    let elapsed = start_time.elapsed().as_secs_f64();
                    logging::log_profile_complete(file_path, profile.row_count, elapsed);

                    let average_row_width = if profile.row_count > 0 {
                        profile.total_row_width as f64 / profile.row_count as f64
                    } else {
                        0.0
                    };

                    println!("Parquet Profile Summary");
                    println!("-----------------------");
                    println!("File: {}", profile.file_path);
                    println!("Format: parquet");
                    println!("Rows: {}", profile.row_count);
                    println!("Columns: {}", profile.column_count);
                    println!("Average Row Width: {:.2} characters", average_row_width);
                    println!("Time Taken: {:.4} seconds", elapsed);
                    println!();

                    print_column_stats(&profile.columns);
                    print_column_warnings(&profile.columns);
                }
                Err(e) => fatal(ProfilerError::ParquetReadError {
                    path: file_path.to_string(),
                    message: e.to_string(),
                }),
            }
        }
        other => fatal(ProfilerError::UnsupportedFormat {
            format: other.to_string(),
        }),
    }
}

fn print_column_stats(columns: &[ColumnProfile]) {
    println!("Column Stats:");
    for col in columns {
        let avg_length = if col.non_null_count > 0 {
            col.total_length as f64 / col.non_null_count as f64
        } else {
            0.0
        };

        let null_pct = if col.total_count > 0 {
            (col.null_count as f64 / col.total_count as f64) * 100.0
        } else {
            0.0
        };

        match col.inferred_type {
            InferredType::Integer | InferredType::Float => {
                let mean = if col.numeric_count > 0 {
                    col.numeric_sum / col.numeric_count as f64
                } else {
                    0.0
                };

                println!(
                    "{} -> type: {}, nulls: {} ({:.2}%), total: {}, min: {}, max: {}, mean: {:.2}",
                    col.name,
                    display_type(&col.inferred_type),
                    col.null_count,
                    null_pct,
                    col.total_count,
                    format_optional_f64(col.numeric_min),
                    format_optional_f64(col.numeric_max),
                    mean
                );
            }
            InferredType::String | InferredType::Mixed => {
                println!(
                    "{} -> type: {}, nulls: {} ({:.2}%), total: {}, min_len: {}, max_len: {}, avg_len: {:.2}",
                    col.name,
                    display_type(&col.inferred_type),
                    col.null_count,
                    null_pct,
                    col.total_count,
                    format_optional_usize(col.min_length),
                    format_optional_usize(col.max_length),
                    avg_length
                );
            }
            _ => {
                println!(
                    "{} -> type: {}, nulls: {} ({:.2}%), total: {}",
                    col.name,
                    display_type(&col.inferred_type),
                    col.null_count,
                    null_pct,
                    col.total_count
                );
            }
        }
    }
}

fn print_column_warnings(columns: &[ColumnProfile]) {
    let mut warnings = Vec::new();

    for col in columns {
        let null_pct = if col.total_count > 0 {
            (col.null_count as f64 / col.total_count as f64) * 100.0
        } else {
            0.0
        };

        if null_pct >= 50.0 {
            warnings.push(format!(
                "{} -> high missingness ({:.2}% null)",
                col.name, null_pct
            ));
        } else if null_pct >= 20.0 {
            warnings.push(format!(
                "{} -> moderate missingness ({:.2}% null)",
                col.name, null_pct
            ));
        }

        if matches!(col.inferred_type, InferredType::Mixed) {
            warnings.push(format!("{} -> mixed or complex type detected", col.name));
        }

        if matches!(
            col.inferred_type,
            InferredType::Integer | InferredType::Float
        ) {
            if let (Some(min), Some(max)) = (col.numeric_min, col.numeric_max) {
                if (max - min).abs() < f64::EPSILON && col.numeric_count > 0 {
                    warnings.push(format!("{} -> constant numeric values detected", col.name));
                }

                if min < 0.0 {
                    warnings.push(format!(
                        "{} -> negative numeric values present (min: {:.2})",
                        col.name, min
                    ));
                }

                if max.abs() > 1_000_000.0 || min.abs() > 1_000_000.0 {
                    warnings.push(format!(
                        "{} -> extreme numeric range detected (min: {:.2}, max: {:.2})",
                        col.name, min, max
                    ));
                }
            }
        }
    }

    if !warnings.is_empty() {
        logging::log_column_warnings(warnings.len());

        println!();
        println!("Column Warnings:");
        for warning in warnings {
            println!("- {}", warning);
        }
    }
}

fn detect_format(file_path: &str) -> String {
    Path::new(file_path)
        .extension()
        .and_then(|e| e.to_str())
        .map(|ext| ext.to_lowercase())
        .unwrap_or_else(|| "unknown".to_string())
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
