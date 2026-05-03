use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProfilerError {
    #[error("File not found: '{path}'. Does the file exist at that path?")]
    FileNotFound { path: String },

    #[error(
        "Unsupported file format: '{format}'. Supported formats are: csv, json, jsonl, ndjson, parquet."
    )]
    UnsupportedFormat { format: String },

    #[error("Failed to parse CSV file '{path}': {message}")]
    CsvParseError { path: String, message: String },

    #[error("Failed to parse JSON file '{path}': {message}")]
    JsonParseError { path: String, message: String },

    #[error("Failed to read Parquet file '{path}': {message}")]
    ParquetReadError { path: String, message: String },

    #[error("Failed to load config file '{path}': {message}")]
    ConfigError { path: String, message: String },

    #[error("Invalid value for {name}: {message}")]
    InvalidArgument { name: String, message: String },
}

impl ProfilerError {
    pub fn hint(&self) -> &'static str {
        match self {
            ProfilerError::FileNotFound { .. } => {
                "Tip: Check the path passed with --file and make sure the file exists."
            }
            ProfilerError::UnsupportedFormat { .. } => {
                "Tip: Use --format csv, --format json, --format jsonl, --format ndjson, or --format parquet."
            }
            ProfilerError::CsvParseError { .. } => {
                "Tip: Check whether the file is valid CSV and whether --delimiter matches the file."
            }
            ProfilerError::JsonParseError { .. } => {
                "Tip: Use a JSON array of objects or NDJSON with one JSON object per line."
            }
            ProfilerError::ParquetReadError { .. } => {
                "Tip: Verify the file is a valid Parquet file and is not corrupted."
            }
            ProfilerError::ConfigError { .. } => {
                "Tip: Check that the config file exists and contains valid TOML."
            }
            ProfilerError::InvalidArgument { .. } => {
                "Tip: Check the CLI arguments and use --help to see valid values."
            }
        }
    }
}

pub fn fatal(err: ProfilerError) -> ! {
    tracing::error!("{}", err);
    eprintln!("Error: {}", err);
    eprintln!("{}", err.hint());
    std::process::exit(1);
}
