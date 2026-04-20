use tracing_subscriber::fmt;
use tracing_subscriber::EnvFilter;

pub fn init_logging(verbose: bool) {
    let filter = if verbose { "info" } else { "warn" };

    fmt()
        .with_env_filter(EnvFilter::new(filter))
        .with_target(false)
        .init();
}