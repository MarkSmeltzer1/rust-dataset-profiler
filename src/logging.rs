use tracing_subscriber::fmt;
use tracing_subscriber::EnvFilter;

pub fn init_logging(verbose: bool) {
    let filter = if verbose { "info" } else { "warn" };

    fmt()
        .with_env_filter(EnvFilter::new(filter))
        .with_target(false)
        .init();

    tracing::info!("Dataset profiler starting");
}

pub fn log_config_loaded(path: &str) {
    tracing::info!("Config loaded from: {}", path);
}

pub fn log_file_open(path: &str) {
    tracing::info!("Opening file: {}", path);
}

pub fn log_format(format: &str, explicit: bool) {
    if explicit {
        tracing::info!("Format set explicitly: {}", format);
    } else {
        tracing::info!("Format auto-detected from extension: {}", format);
    }
}

pub fn log_threads(threads: usize) {
    tracing::info!(
        "Thread setting: {}. Current profiling readers are streaming and mostly single-threaded.",
        threads
    );
}

pub fn log_dry_run_start(path: &str) {
    tracing::info!("Dry-run mode enabled for: {}", path);
}

pub fn log_dry_run_complete(elapsed: f64) {
    tracing::info!("Dry run completed in {:.4} seconds", elapsed);
}

pub fn log_profile_start(path: &str, format: &str) {
    tracing::info!("Starting {} profiling for: {}", format, path);
}

pub fn log_profile_complete(path: &str, rows: usize, elapsed: f64) {
    tracing::info!(
        "Profiling completed for '{}': {} rows processed in {:.4} seconds",
        path,
        rows,
        elapsed
    );
}

pub fn log_malformed_rows(count: usize) {
    if count > 0 {
        tracing::warn!("{} malformed row(s) detected", count);
    }
}

pub fn log_column_warnings(count: usize) {
    if count > 0 {
        tracing::warn!("{} column warning(s) flagged", count);
    }
}
