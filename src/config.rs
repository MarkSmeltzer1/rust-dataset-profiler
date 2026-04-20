use serde::Deserialize;
use std::error::Error;
use std::fs;

#[derive(Debug, Deserialize, Default)]
pub struct AppConfig {
    pub format: Option<String>,
    pub delimiter: Option<char>,
    pub verbose: Option<bool>,
    pub dry_run: Option<bool>,
}

pub fn load_config(path: &str) -> Result<AppConfig, Box<dyn Error>> {
    let content = fs::read_to_string(path)?;
    let config: AppConfig = toml::from_str(&content)?;
    Ok(config)
}