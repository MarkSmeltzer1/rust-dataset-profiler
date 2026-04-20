mod cli;
mod readers;
mod types;

use clap::Parser;
use cli::Cli;
use readers::csv::profile_csv;

fn main() {
    let args = Cli::parse();

    println!("Dataset Profiler Starting...");
    println!("File: {}", args.file);

    if let Some(format) = &args.format {
        println!("Format: {}", format);
    } else {
        println!("Format: auto-detect");
    }

    println!("Delimiter: {}", args.delimiter);
    println!("Verbose: {}", args.verbose);
    println!("Dry Run: {}", args.dry_run);
    println!();

    let detected_format = match &args.format {
        Some(fmt) => fmt.to_lowercase(),
        None => detect_format(&args.file),
    };

    match detected_format.as_str() {
        "csv" => match profile_csv(&args.file, args.delimiter as u8) {
            Ok(profile) => {
                println!("CSV Profile Summary");
                println!("-------------------");
                println!("File: {}", profile.file_path);
                println!("Rows: {}", profile.row_count);
                println!("Columns: {}", profile.column_count);
                println!();

                println!("Column Stats:");
                for col in profile.columns {
                    println!(
                        "{} -> type: {:?}, nulls: {}, total: {}",
                        col.name, col.inferred_type, col.null_count, col.total_count
                    );
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