mod cli;
mod logging;
mod readers;
mod types;

use clap::Parser;
use cli::Cli;
use readers::csv::{preview_csv, profile_csv};
use tracing::{info, warn};
use types::InferredType;

fn main() {
    let args = Cli::parse();

    logging::init_logging(args.verbose);

    info!("Starting dataset profiler");
    info!("Input file: {}", args.file);

    let detected_format = match &args.format {
        Some(fmt) => fmt.to_lowercase(),
        None => detect_format(&args.file),
    };

    info!("Detected format: {}", detected_format);

    if args.dry_run {
        info!("Dry-run mode enabled");

        match detected_format.as_str() {
            "csv" => match preview_csv(&args.file, args.delimiter as u8) {
                Ok(preview) => {
                    println!("Dataset Profiler Dry Run");
                    println!("------------------------");
                    println!("File: {}", preview.file_path);
                    println!("Format: csv");
                    println!("Columns: {}", preview.column_count);
                    println!("Headers: {:?}", preview.headers);
                    println!("Dry run complete. Full profiling was skipped.");
                }
                Err(e) => {
                    eprintln!("Error during dry run: {}", e);
                    std::process::exit(1);
                }
            },
            _ => {
                eprintln!("Unsupported format: {}", detected_format);
                std::process::exit(1);
            }
        }

        return;
    }

    match detected_format.as_str() {
        "csv" => match profile_csv(&args.file, args.delimiter as u8) {
            Ok(profile) => {
                info!("CSV profiling completed successfully");

                println!("CSV Profile Summary");
                println!("-------------------");
                println!("File: {}", profile.file_path);
                println!("Format: csv");
                println!("Delimiter: {}", args.delimiter);
                println!("Rows: {}", profile.row_count);
                println!("Columns: {}", profile.column_count);
                println!("Malformed Rows: {}", profile.malformed_row_count);
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

                println!("Column Stats:");
                for col in profile.columns {
                    let avg_length = if col.non_null_count > 0 {
                        col.total_length as f64 / col.non_null_count as f64
                    } else {
                        0.0
                    };

                    match col.inferred_type {
                        InferredType::Integer | InferredType::Float => {
                            println!(
                                "{} -> type: {}, nulls: {}, total: {}, min: {:?}, max: {:?}",
                                col.name,
                                display_type(&col.inferred_type),
                                col.null_count,
                                col.total_count,
                                col.numeric_min,
                                col.numeric_max
                            );
                        }
                        InferredType::String | InferredType::Mixed => {
                            println!(
                                "{} -> type: {}, nulls: {}, total: {}, min_len: {:?}, max_len: {:?}, avg_len: {:.2}",
                                col.name,
                                display_type(&col.inferred_type),
                                col.null_count,
                                col.total_count,
                                col.min_length,
                                col.max_length,
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
            Err(e) => {
                eprintln!("Error profiling CSV: {}", e);
                std::process::exit(1);
            }
        },
        _ => {
            eprintln!("Unsupported format: {}", detected_format);
            std::process::exit(1);
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